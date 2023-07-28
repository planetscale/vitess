package domain

import (
	"context"

	"go.uber.org/zap"
	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/boostrpc/packet"
	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/dataflow/domain/replay"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/dataflow/processing"
	"vitess.io/vitess/go/boost/sql"
)

func (d *Domain) evictionWalkPath(ctx context.Context, path []*packet.ReplayPathSegment, keys *[]sql.Row, tag dataflow.Tag, ex processing.Executor) error {
	from := path[0].Node
	for _, seg := range path {
		if err := d.nodes.Get(seg.Node).ProcessEviction(ctx, from, seg.PartialKey, keys, tag, d.shard, ex); err != nil {
			return err
		}
		from = seg.Node
	}
	return nil
}

func (d *Domain) triggerDownstreamEvictions(ctx context.Context, keyCols []int, keys []sql.Row, node dataflow.LocalNodeIdx, ex processing.Executor) error {
	for tag, path := range d.replayPaths {
		if path.Source == node {
			// Check whether this replay path is for the same key.
			switch path.Trigger.Kind {
			case replay.TriggerLocal, replay.TriggerStart:
				if !slices.Equal(path.Trigger.Cols, keyCols) {
					continue
				}
			default:
				panic("unreachable")
			}

			keys := slices.Clone(keys)
			if err := d.evictionWalkPath(ctx, path.Path, &keys, tag, ex); err != nil {
				return err
			}

			if path.Trigger.Kind == replay.TriggerLocal {
				target := path.Path[len(path.Path)-1]
				if d.nodes.Get(target.Node).IsReader() {
					// already evicted from in walk_path
					continue
				}
				if !d.state.ContainsKey(target.Node) {
					// this is probably because
					if _, notready := d.notReady[target.Node]; !notready {
						d.log.Warn("got eviction for ready but stateless node", target.Node.Zap())
					}
					continue
				}

				d.log.Debug("downstream eviction for node", target.Node.Zap(), tag.Zap(), zap.Int("key_count", len(keys)))

				d.state.Get(target.Node).EvictKeys(tag, keys)
				if err := d.triggerDownstreamEvictions(ctx, target.PartialKey, keys, target.Node, ex); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (d *Domain) handleEvictAny(ctx context.Context, pkt *packet.EvictRequest, ex processing.Executor) error {
	type nodesize struct {
		node dataflow.LocalNodeIdx
		size int64
	}
	var nodes []nodesize

	if pkt.Node == dataflow.InvalidLocalNode {
		d.nodes.ForEach(func(local dataflow.LocalNodeIdx, n *flownode.Node) bool {
			if r := n.AsReader(); r != nil && r.IsPartial() {
				if sz := r.StateSizeAtomic().Load(); sz > 0 {
					nodes = append(nodes, nodesize{local, sz})
				}
			} else if st := d.state.Get(local); st != nil && st.IsPartial() {
				if sz := st.StateSizeAtomic().Load(); sz > 0 {
					nodes = append(nodes, nodesize{local, sz})
				}
			}
			return true
		})

		slices.SortFunc(nodes, func(a, b nodesize) int {
			// Sort in reverse order.
			switch {
			case a.size > b.size:
				return -1
			case a.size < b.size:
				return 1
			default:
				return 0
			}
		})

		// TODO: we don't want to evict from all nodes in this domain; Noria just truncates
		// 	the list of nodes to 3, but can we be smarter about this?

		n := int64(len(nodes))
		for i := len(nodes) - 1; i >= 0; i-- {
			target := &nodes[i]
			share := (pkt.NumBytes + n - 1) / n

			if n > 1 && target.size/2 < share {
				target.size = target.size / 2
			} else {
				target.size = share
			}

			pkt.NumBytes -= target.size
			n--
		}
	} else {
		nodes = append(nodes, nodesize{node: pkt.Node, size: pkt.NumBytes})
	}

	for _, target := range nodes {
		var n = d.nodes.Get(target.node)

		if n.IsDropped() {
			continue
		}

		d.log.Debug("random eviction from node", target.node.Zap(), zap.Int64("memory", target.size))

		if r := n.AsReader(); r != nil {
			if !r.IsPartial() {
				panic("trying to evict from a fully materialized node")
			}
			r.EvictRandomKeys(d.rng, target.size)
		} else {
			st := d.state.Get(target.node)
			if !st.IsPartial() {
				panic("trying to evict from a fully materialized node")
			}

			keyCols, keys := st.EvictRandomKeys(d.rng, target.size)
			if len(keys) > 0 {
				if err := d.triggerDownstreamEvictions(ctx, keyCols, keys, target.node, ex); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (d *Domain) handleEvictKeys(ctx context.Context, pkt *packet.EvictKeysRequest, executor processing.Executor) error {
	dst := pkt.Link.Dst
	keys := &pkt.Keys
	tag := pkt.Tag

	rp, ok := d.replayPaths[tag]
	if !ok {
		d.log.Warn("eviction for tag that has not yet been finalized", tag.Zap())
		return nil
	}

	trigger := rp.Trigger
	path := rp.Path

	i := slices.IndexFunc(path, func(ps *packet.ReplayPathSegment) bool { return ps.Node == dst })
	if err := d.evictionWalkPath(ctx, path[i:], keys, tag, executor); err != nil {
		return err
	}

	switch trigger.Kind {
	case replay.TriggerEnd, replay.TriggerLocal, replay.TriggerExternal:
		// This path terminates inside the domain. Find the target node, evict
		// from it, and then propagate the eviction further downstream.
		target := path[len(path)-1].Node
		if d.nodes.Get(target).IsReader() {
			return nil
		}
		// No need to continue if node was dropped.
		if d.nodes.Get(target).IsDropped() {
			return nil
		}
		if evicted := d.state.Get(target).EvictKeys(tag, *keys); evicted != nil {
			return d.triggerDownstreamEvictions(ctx, evicted, *keys, target, executor)
		}
	}
	return nil
}

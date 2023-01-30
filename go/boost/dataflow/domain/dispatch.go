package domain

import (
	"context"

	"go.uber.org/zap"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/boostrpc/packet"
	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/dataflow/domain/replay"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/dataflow/processing"
	"vitess.io/vitess/go/boost/sql"
)

func (d *Domain) dispatch(ctx context.Context, m packet.ActiveFlowPacket, executor processing.Executor) error {
	src := m.Src()
	me := m.Dst()

	if fwd := d.replaying; fwd != nil {
		if fwd.To == me {
			fwd.Buffered.PushBack(m)
			return nil
		}
	}

	if _, notready := d.notReady[me]; notready {
		d.log.Warn("dispatch not ready", me.Zap())
		return nil
	}

	if gt := d.gtidTrackers.Get(me); gt != nil {
		if err := gt.Process(ctx, &m); err != nil {
			return err
		}
	}

	if msg := m.GetMessage(); msg != nil && len(msg.SeenTags) > 0 {
		if tagging, ok := d.filterReplayPathByDst[me]; ok {
			msg.Records = sql.FilterRecords(msg.Records, func(r sql.Record) bool {
				if r.Offset < 0 {
					return true
				}
				for _, s := range msg.SeenTags {
					if s.Offset == r.Offset && tagging[s.Tag] {
						if log := d.log.Check(zap.DebugLevel, "filtering out packet"); log != nil {
							log.Write(me.Zap(), r.Row.Zap("row"))
						}
						return false
					}
				}
				return true
			})
			if msg.Records == nil {
				return nil
			}
		}
	}

	var input = flownode.FlowInput{
		Packet:     m,
		KeyedBy:    nil,
		State:      d.state,
		Nodes:      d.nodes,
		OnShard:    d.shard,
		Swap:       true,
		ReplayPath: nil,
		Ex:         executor,
	}
	var output flownode.FlowOutput

	n := d.nodes.Get(me)
	if err := n.Process(ctx, &input, &output); err != nil {
		return err
	}

	if input.Packet.Inner == nil {
		// no need to deal with our children if we're not sending them anything
		return nil
	}

	// normally, we ignore misses during regular forwarding.
	// however, we have to be a little careful in the case of joins.
	var evictions map[dataflow.Tag]map[sql.Row]bool
	if slices.ContainsFunc(output.Misses, func(miss processing.Miss) bool { return miss.Flush }) {
		// TODO: support evictions on fully materialized nodes
		if st := d.state.Get(me); st != nil && !st.IsPartial() {
			for tag, rp := range d.replayPaths {
				if rp.Trigger.Kind == replay.TriggerExternal && rp.Path[0].Node == me {
					replayCtx := &packet.ReplayPiece_Replace{
						Replace: &packet.ReplayPiece_ContextReplace{
							OriginalLink: input.Packet.Link().Clone(),
						},
					}
					link := &packet.Link{Src: me, Dst: me}
					return d.sendReplayUnified(ctx, tag, link, rp.Upquery, replayCtx, nil)
				}
			}
			panic("could not resolve external upquery after fully materialized miss")
		}

		// there are two possible cases here:
		//
		//  - this is a write that will hit a hole in every downstream materialization.
		//    dropping it is totally safe!
		//  - this is a write that will update an entry in some downstream materialization.
		//    this is *not* allowed! we *must* ensure that downstream remains up to date.
		//    but how can we? we missed in the other side of the join, so we can't produce
		//    the necessary output record... what we *can* do though is evict from any
		//    downstream, and then we guarantee that we're in case 1!
		//
		// if you're curious about how we may have ended up in case 2 above, here are two
		// ways:
		//
		//  - some downstream view is partial over the join key. some time in the past, it
		//    requested a replay of key k. that replay produced *no* rows from the side
		//    that was replayed. this in turn means that no lookup was performed on the
		//    other side of the join, and so k wasn't replayed to that other side (which
		//    then still has a hole!). in that case, any subsequent write with k in the
		//    join column from the replay side will miss in the other side.
		//  - some downstream view is partial over a column that is *not* the join key. in
		//    the past, it replayed some key k, which means that we aren't allowed to drop
		//    any write with k in that column. now, a write comes along with k in that
		//    replay column, but with some hitherto unseen key z in the join column. if the
		//    replay of k never caused a lookup of z in the other side of the join, then
		//    the other side will have a hole. thus, we end up in the situation where we
		//    need to forward a write through the join, but we miss.
		//
		// unfortunately, we can't easily distinguish between the case where we have to
		// evict and the case where we don't (at least not currently), so we *always* need
		// to evict when this happens. this shouldn't normally be *too* bad, because writes
		// are likely to be dropped before they even reach the join in most benign cases
		// (e.g., in an ingress). this can be remedied somewhat in the future by ensuring
		// that the first of the two causes outlined above can't happen (by always doing a
		// lookup on the replay key, even if there are now rows). then we know that the
		// *only* case where we have to evict is when the replay key != the join key.

		// first, what partial replay paths go through this node?
		from := d.nodes.Get(src).GlobalAddr()

		type tagkeys struct {
			tag  dataflow.Tag
			keys []int
		}

		var deps []tagkeys

		for tag, rp := range d.replayPaths {
		nextSegment:
			for _, rps := range rp.Path {
				if rps.Node == me {
					keys := rps.PartialKey
					// we need to find the *input* column that produces that output.
					//
					// if one of the columns for this replay path's keys does not
					// resolve into the ancestor we got the update from, we don't need
					// to issue an eviction for that path. this is because we *missed*
					// on the join column in the other side, so we *know* it can't have
					// forwarded anything related to the write we're now handling.
					var cols []int
					for _, k := range keys {
						parentColumns := n.ParentColumns(k)
						idx := slices.IndexFunc(parentColumns, func(nc flownode.NodeColumn) bool { return nc.Node == from })
						if idx < 0 {
							continue nextSegment
						}
						cols = append(cols, parentColumns[idx].Column)
					}
					deps = append(deps, tagkeys{tag: tag, keys: cols})
				}
			}
		}

		// map[sql.Row]: safe, used for iteration
		evictions = make(map[dataflow.Tag]map[sql.Row]bool)
		for _, miss := range output.Misses {
			if !miss.Flush {
				continue
			}
			for _, tk := range deps {
				rb, ok := evictions[tk.tag]
				if !ok {
					rb = make(map[sql.Row]bool)
					evictions[tk.tag] = rb
				}

				var misskey = sql.NewRowBuilder(len(tk.keys))
				for _, key := range tk.keys {
					misskey.Add(miss.Record.Row.ValueAt(key))
				}
				rb[misskey.Finish()] = true
			}
		}
	}

	for tag, keys := range evictions {
		var pkt = &packet.EvictKeysRequest{
			Keys: maps.Keys(keys),
			Link: &packet.Link{Src: src, Dst: me},
			Tag:  tag,
		}
		if err := d.handleEvictKeys(ctx, pkt, executor); err != nil {
			return err
		}
	}

	switch input.Packet.Inner.(type) {
	case *packet.Message:
		if input.Packet.IsEmpty() {
			return nil
		}
	case *packet.ReplayPiece:
		panic("replay should never go through dispatch")
	default:
		panic("???")
	}

	for _, child := range d.nodes.Get(me).Children() {
		childIsMerger := d.nodes.Get(child).IsShardMerger()

		m := input.Packet.Clone()

		if childIsMerger {
			// we need to preserve the egress src (which includes shard identifier)
		} else {
			m.Link().Src = me
		}
		m.Link().Dst = child
		if err := d.dispatch(ctx, m, executor); err != nil {
			return err
		}
	}

	return nil
}

package domain

import (
	"vitess.io/vitess/go/boost/boostrpc/packet"
	"vitess.io/vitess/go/boost/common"
	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/dataflow/domain/replay"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/dataflow/state"
	"vitess.io/vitess/go/boost/dataflow/view"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/server/controller/boostplan/upquery"
)

func (d *Domain) handleReady(rdy *packet.ReadyRequest, done chan struct{}) error {
	node := rdy.Node

	if d.replaying != nil {
		d.log.Panic("called ReadyRequest on replaying node", node.Zap())
	}

	n := d.nodes.Get(node)

	if len(rdy.Index) > 0 {
		if n.IsTable() {
			d.log.Debug("setting up external table")
		} else {
			s := state.NewMemoryState()
			for _, idx := range rdy.Index {
				s.AddKey(idx.Key, n.Schema(), nil, idx.IsPrimary)
			}
			d.state.Insert(node, s)
			d.memstats.Register(d.index, d.shard, n.GlobalAddr(), s.StateSizeAtomic())
		}
	}
	// NOTE: just because index_on is None does *not* mean we're not
	// materialized

	delete(d.notReady, node)

	if r := n.AsReader(); r != nil {
		if st := r.Writer(); st != nil {
			st.Swap()
		}
	}

	close(done)
	return nil
}

func (d *Domain) handleUpdateEgress(egress *packet.UpdateEgressRequest) error {
	node := egress.Node
	e := d.nodes.Get(node).AsEgress()

	if newtx := egress.NewTx; newtx != nil {
		e.AddTx(graph.NodeIdx(newtx.Node), dataflow.LocalNodeIdx(newtx.Local),
			dataflow.DomainAddr{
				Domain: newtx.Domain.Domain,
				Shard:  newtx.Domain.Shard,
			})
	}
	if newtag := egress.NewTag; newtag != nil {
		e.AddTag(newtag.Tag, newtag.Node)
	}
	return nil
}

func (d *Domain) handleUpdateSharder(update *packet.UpdateSharderRequest) error {
	n := d.nodes.Get(update.Node)
	if sharder := n.AsSharder(); sharder != nil {
		for _, tx := range update.NewTxs {
			sharder.AddShardedTx(tx)
		}
	}
	return nil
}

func (d *Domain) handleAddNode(addNode *packet.AddNodeRequest) error {
	node := flownode.NodeFromProto(addNode.Node)
	addr := node.LocalAddr()
	d.notReady[addr] = struct{}{}

	for _, p := range addNode.Parents {
		d.nodes.Get(p).AddChild(addr)
	}
	d.nodes.Insert(addr, node)
	return node.OnDeploy()
}

func (d *Domain) handleRemoveNodes(pkt *packet.RemoveNodesRequest) error {
	var deleted []*view.Writer
	for _, node := range pkt.Nodes {
		n := d.nodes.Get(node)
		if reader := n.AsReader(); reader != nil {
			readerID := ReaderID{
				Node:  n.GlobalAddr(),
				Shard: d.shardn(),
			}
			d.readers.Delete(readerID)
			deleted = append(deleted, reader.Writer())
		}

		n.Remove()
		d.nodes.Remove(node)

		d.log.Debug("node removed", node.Zap())
	}

	for _, node := range pkt.Nodes {
		d.nodes.ForEach(func(_ dataflow.LocalNodeIdx, n *flownode.Node) bool {
			n.TryRemoveChild(node)
			// NOTE: since nodes are always removed leaves-first, it's not
			// important to update parent pointers here
			return true
		})
	}

	go func() {
		for _, w := range deleted {
			w.Free()
		}
	}()

	return nil
}

func (d *Domain) handleSetupReplayPath(setup *packet.SetupReplayPathRequest, done chan struct{}) error {
	// let coordinator know that we've registered the tagged path
	// TODO@vmg: the original Noria code notifies right at the start of the function;
	// 		is there a reason why this doesn't happen at the end?
	close(done)

	d.log.Debug("SetupReplayPathRequest", setup.Source.Zap(), setup.Tag.Zap())

	trigger, err := replay.TriggerEndpointFromProto(setup.Trigger, d.coordinator, d.shard)
	if err != nil {
		return err
	}

	var up *upquery.Upquery

	tag := setup.Tag
	path := setup.Path
	last := path[len(path)-1]

	if setup.LastSegment {
		d.log.Debug("tagging node as destination for replay path", d.nodes.Get(last.Node).Index().Global.Zap(), tag.Zap())
		seen, ok := d.filterReplayPathByDst[last.Node]
		if !ok {
			seen = make(map[dataflow.Tag]bool)
			d.filterReplayPathByDst[last.Node] = seen
		}
		seen[tag] = true
	}

	keepReplayDst := func() {
		rpath, ok := d.replayPathsByDst[last.Node]
		if !ok {
			rpath = make(map[common.Columns][]dataflow.Tag)
			d.replayPathsByDst[last.Node] = rpath
		}

		partialKey := common.ColumnsFrom(last.PartialKey)
		rpath[partialKey] = append(rpath[partialKey], tag)
	}

	switch trigger.Kind {
	case replay.TriggerNone:
		if setup.Upquery != "" {
			up, err = upquery.Parse(setup.Upquery, nil, false, d.upqueryMode)
			if err != nil {
				return err
			}
		}

	case replay.TriggerEnd, replay.TriggerLocal:
		keepReplayDst()

	case replay.TriggerExternal:
		keepReplayDst()

		up, err = upquery.Parse(setup.Upquery, trigger.Cols, true, d.upqueryMode)
		if err != nil {
			return err
		}

		if setup.Source != dataflow.ExternalSource {
			panic("invalid source for External replay path")
		}
		n := d.nodes.Get(last.Node)
		parent := d.nodes.Get(n.Parents()[0])

		gt := d.gtidTrackers.Get(last.Node)
		if gt == nil {
			gt = flownode.NewGtidTracker(parent.Schema(), n.Schema())
			d.gtidTrackers.Insert(last.Node, gt)
		}

		d.log.Debug("created gtid tracker for trigger", n.Index().Global.Zap(), tag.Zap())
		if err := gt.AddTag(n, tag, up.Key()); err != nil {
			return err
		}

	case replay.TriggerStart:
		if setup.Source == dataflow.InvalidLocalNode {
			panic("invalid source for Start replay path")
		}
		n := d.nodes.Get(setup.Source)
		if external := n.AsTable(); external != nil {
			up, err = upquery.Parse(setup.Upquery, trigger.Cols, false, d.upqueryMode)
			if err != nil {
				return err
			}

			gt := d.gtidTrackers.Get(setup.Source)
			if gt == nil {
				gt = flownode.NewGtidTracker(external.Schema(), external.Schema())
				d.gtidTrackers.Insert(setup.Source, gt)
			}

			d.log.Debug("created gtid tracker for external table", n.Index().Global.Zap(), tag.Zap())
			if err := gt.AddTag(n, tag, trigger.Cols); err != nil {
				return err
			}
		}
	}

	d.replayPaths[tag] = &replay.Path{
		Source:                setup.Source,
		Path:                  path,
		NotifyDone:            setup.NotifyDone,
		PartialUnicastSharder: setup.PartialUnicastSharder,
		Trigger:               trigger,
		Upquery:               up,
	}
	return nil
}

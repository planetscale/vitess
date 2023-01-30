package flownode

import (
	"context"

	"vitess.io/vitess/go/boost/boostrpc/packet"
	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/dataflow/flownode/flownodepb"
	"vitess.io/vitess/go/boost/dataflow/processing"
	"vitess.io/vitess/go/boost/graph"
)

type Ingress struct{}

func (*Ingress) dataflow() {}

func (*Ingress) ToProto() *flownodepb.Node_Ingress {
	return &flownodepb.Node_Ingress{}
}

func NewIngressFromProto(_ *flownodepb.Node_Ingress) *Ingress {
	return &Ingress{}
}

type EgressTx struct {
	node  graph.NodeIdx
	local dataflow.LocalNodeIdx
	dest  dataflow.DomainAddr
}

type Egress struct {
	txs  []EgressTx
	tags map[dataflow.Tag]graph.NodeIdx
}

func (e *Egress) dataflow() {}

func (e *Egress) Process(ctx context.Context, m *packet.ActiveFlowPacket, shard uint, output processing.Executor) error {
	if len(e.txs) == 0 {
		panic("Egress.Process without any transactions")
	}

	// send any queued updates to all external children
	txn := len(e.txs) - 1

	var replayTo graph.NodeIdx = graph.InvalidNode
	if t := m.Tag(); t != dataflow.TagNone {
		var ok bool
		replayTo, ok = e.tags[t]
		if !ok {
			panic("egress node told about replay message, but not on replay path")
		}
	}

	for txi, tx := range e.txs {
		last := txi == txn
		if replayTo != graph.InvalidNode {
			if replayTo == tx.node {
				last = true
			} else {
				continue
			}
		}

		var data packet.ActiveFlowPacket
		if last {
			data = m.Take()
		} else {
			data = m.Clone()
		}

		// src is usually ignored and overwritten by ingress
		// *except* if the ingress is marked as a shard merger
		// in which case it wants to know about the shard
		data.Link().Src = dataflow.LocalNodeIdx(shard)
		data.Link().Dst = tx.local

		if err := output.Send(ctx, tx.dest, data.Inner); err != nil {
			return err
		}
		if last {
			break
		}
	}
	return nil
}

func (e *Egress) AddTx(dstG graph.NodeIdx, dstL dataflow.LocalNodeIdx, addr dataflow.DomainAddr) {
	e.txs = append(e.txs, EgressTx{node: dstG, local: dstL, dest: addr})
}

func (e *Egress) AddTag(tag dataflow.Tag, dst graph.NodeIdx) {
	e.tags[tag] = dst
}

func NewEgress() *Egress {
	return &Egress{
		tags: make(map[dataflow.Tag]graph.NodeIdx),
	}
}

func (e *Egress) ToProto() *flownodepb.Node_Egress {
	if len(e.txs) > 0 || len(e.tags) > 0 {
		panic("unsupported: serializing stateful egress")
	}
	return &flownodepb.Node_Egress{}
}

func NewEgressFromProto(_ *flownodepb.Node_Egress) *Egress {
	return NewEgress()
}

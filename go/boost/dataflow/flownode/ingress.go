package flownode

import (
	"context"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/dataflow/processing"
	"vitess.io/vitess/go/boost/dataflow/trace"
	"vitess.io/vitess/go/boost/graph"
)

type Ingress struct{}

func (*Ingress) dataflow() {}

func (*Ingress) ToProto() *boostpb.Node_Ingress {
	return &boostpb.Node_Ingress{}
}

func NewIngressFromProto(_ *boostpb.Node_Ingress) *Ingress {
	return &Ingress{}
}

type EgressTx struct {
	node  graph.NodeIdx
	local boostpb.LocalNodeIndex
	dest  boostpb.DomainAddr
}

type Egress struct {
	txs  []EgressTx
	tags map[boostpb.Tag]graph.NodeIdx
}

func (e *Egress) dataflow() {}

func (e *Egress) Process(ctx context.Context, m **boostpb.Packet, shard uint, output processing.Executor) error {
	if trace.T {
		var span *trace.Span
		ctx, span = trace.WithSpan(ctx, "Egress.Process", *m)
		defer span.Close()
	}

	if len(e.txs) == 0 {
		panic("Egress.Process without any transactions")
	}

	// send any queued updates to all external children
	txn := len(e.txs) - 1

	var replayTo graph.NodeIdx = graph.InvalidNode
	if t := (*m).Tag(); t != boostpb.TagNone {
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

		var pkt *boostpb.Packet
		if last {
			pkt = *m
			*m = nil
		} else {
			pkt = (*m).CloneData()
		}

		// src is usually ignored and overwritten by ingress
		// *except* if the ingress is marked as a shard merger
		// in which case it wants to know about the shard
		pkt.Link().Src = boostpb.LocalNodeIndex(shard)
		pkt.Link().Dst = tx.local

		if err := output.Send(ctx, tx.dest, pkt); err != nil {
			return err
		}
		if last {
			break
		}
	}
	return nil
}

func (e *Egress) AddTx(dstG graph.NodeIdx, dstL boostpb.LocalNodeIndex, addr boostpb.DomainAddr) {
	e.txs = append(e.txs, EgressTx{node: dstG, local: dstL, dest: addr})
}

func (e *Egress) AddTag(tag boostpb.Tag, dst graph.NodeIdx) {
	e.tags[tag] = dst
}

func NewEgress() *Egress {
	return &Egress{
		tags: make(map[boostpb.Tag]graph.NodeIdx),
	}
}

func (e *Egress) ToProto() *boostpb.Node_Egress {
	if len(e.txs) > 0 || len(e.tags) > 0 {
		panic("unsupported: serializing stateful egress")
	}
	return &boostpb.Node_Egress{}
}

func NewEgressFromProto(_ *boostpb.Node_Egress) *Egress {
	return NewEgress()
}

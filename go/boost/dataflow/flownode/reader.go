package flownode

import (
	"context"
	"math/rand"
	"sync/atomic"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/boostrpc/packet"
	"vitess.io/vitess/go/boost/dataflow/flownode/flownodepb"
	"vitess.io/vitess/go/boost/dataflow/view"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/server/controller/boostplan/viewplan"
	"vitess.io/vitess/go/boost/sql"
)

var _ NodeImpl = (*Reader)(nil)

type Reader struct {
	// not serialized
	writer *view.Writer

	publicID string
	forNode  graph.NodeIdx
	plan     *viewplan.Plan
}

func (r *Reader) dataflow() {}

func (r *Reader) Key() []int {
	return r.plan.TriggerKey
}

func traceParentOrdering(g *graph.Graph[*Node], node graph.NodeIdx) (Order, int) {
	n := g.Value(node)
	switch parent := n.impl.(type) {
	case *TopK:
		return slices.Clone(parent.order), parent.k
	case *Project:
		order, topk := traceParentOrdering(g, n.Ancestors()[0])
		return parent.projectOrder(order), topk
	case *Filter:
		return traceParentOrdering(g, n.Ancestors()[0])
	case *Ingress, *Sharder, *Egress:
		return traceParentOrdering(g, g.NeighborsDirected(node, graph.DirectionIncoming).First())
	default:
		return nil, 0
	}
}

func (r *Reader) OnConnected(g *graph.Graph[*Node]) {
	r.plan.TopkOrder, r.plan.TopkLimit = traceParentOrdering(g, r.forNode)
}

func (r *Reader) Order() (cols []int64, desc []bool, limit int) {
	for _, ord := range r.plan.TopkOrder {
		cols = append(cols, int64(ord.Col))
		desc = append(desc, ord.Desc)
	}
	return cols, desc, r.plan.TopkLimit
}

func (r *Reader) IsFor() graph.NodeIdx {
	return r.forNode
}

func (r *Reader) IsMaterialized() bool {
	return r.plan.TriggerKey != nil
}

func (r *Reader) IsPartial() bool {
	return r.writer != nil && r.writer.IsPartial()
}

func (r *Reader) SetWriteHandle(w *view.Writer) {
	if r.writer != nil {
		panic("tried to replace backlog.Writer in Reader")
	}
	r.writer = w
}

func (r *Reader) IsEmpty() bool {
	if r.writer != nil {
		return r.writer.IsEmpty()
	}
	return true
}

func (r *Reader) Process(ctx context.Context, m *packet.ActiveFlowPacket, swap bool) error {
	if st := r.writer; st != nil {
		isRegular := m.GetMessage() != nil
		if isRegular && st.IsPartial() {
			m.FilterRecords(func(row sql.Record) bool {
				// state is already present,
				// so we can safely keep it up to date.
				return st.EntryFromRecord(row.Row).Found()
			})
		}

		// it *can* happen that multiple readers miss (and thus request replay for) the
		// same hole at the same time. we need to make sure that we ignore any such
		// duplicated replay.
		if !isRegular && st.IsPartial() {
			m.FilterRecords(func(row sql.Record) bool {
				if st.EntryFromRecord(row.Row).Found() {
					// a given key should only be replayed to once!
					return false
				} else {
					// filling a hole with replay -- ok
					return true
				}
			})
		}

		dataToAdd := m.TakeRecords()
		st.Add(dataToAdd)
		if swap {
			st.Swap()
		}
	}
	return nil
}

func (r *Reader) Writer() *view.Writer {
	return r.writer
}

func (r *Reader) Shard(factor uint) {
	// noop
}

func (r *Reader) OnEviction(keys []sql.Row) {
	// NOTE: *could* be None if reader has been created but its state hasn't been built yet
	if w := r.writer; w != nil {
		for _, k := range keys {
			w.WithKey(k).MarkHole()
		}
		w.Swap()
	}
}

func (r *Reader) ViewPlan() *viewplan.Plan {
	return r.plan
}

func (r *Reader) PublicColumnLength() int {
	return r.plan.ColumnsForUser
}

func (r *Reader) KeySchema() []sql.Type {
	return r.writer.KeySchema()
}

func (r *Reader) StateSizeAtomic() *atomic.Int64 {
	return r.writer.StateSizeAtomic()
}

func (r *Reader) EvictRandomKeys(rng *rand.Rand, bytesToEvict int64) {
	r.writer.EvictRandomKeys(rng, bytesToEvict)
	r.writer.Swap()
}

func NewReader(forNode graph.NodeIdx, publicID string, plan *viewplan.Plan) *Reader {
	return &Reader{
		forNode:  forNode,
		publicID: publicID,
		plan:     plan,
	}
}

func (r *Reader) ToProto() *flownodepb.Node_Reader {
	return &flownodepb.Node_Reader{
		PublicId: r.publicID,
		ForNode:  r.forNode,
		Plan:     r.plan,
	}
}

func (r *Reader) PublicID() string {
	return r.publicID
}

func NewReaderFromProto(r *flownodepb.Node_Reader) *Reader {
	return &Reader{
		writer:   nil,
		publicID: r.PublicId,
		forNode:  r.ForNode,
		plan:     r.Plan,
	}
}

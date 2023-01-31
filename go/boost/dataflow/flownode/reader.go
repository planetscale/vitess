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
	"vitess.io/vitess/go/boost/sql"
)

var _ NodeImpl = (*Reader)(nil)

type ViewParameter = flownodepb.ViewParameter

type Reader struct {
	// not serialized
	writer *view.Writer

	publicID   string
	forNode    graph.NodeIdx
	state      []int
	parameters []ViewParameter

	topkOrder Order
	topkLimit int

	columnsForView int
	columnsForUser int
}

func (r *Reader) dataflow() {}

func (r *Reader) Key() []int {
	return r.state
}

func (r *Reader) OnConnected(ingredients *graph.Graph[*Node], key []int, parameters []ViewParameter, colLen int) {
	r.state = slices.Clone(key)
	r.parameters = slices.Clone(parameters)
	r.columnsForUser = colLen
	r.columnsForView = colLen

	switch parent := ingredients.Value(r.forNode).impl.(type) {
	case *TopK:
		r.topkOrder = slices.Clone(parent.order)
		r.topkLimit = int(parent.k)

		for _, ord := range r.topkOrder {
			if ord.Col >= r.columnsForUser {
				r.columnsForView = 0
			}
		}
	}
}

func (r *Reader) Order() (cols []int64, desc []bool, limit int) {
	for _, ord := range r.topkOrder {
		cols = append(cols, int64(ord.Col))
		desc = append(desc, ord.Desc)
	}
	return cols, desc, r.topkLimit
}

func (r *Reader) IsFor() graph.NodeIdx {
	return r.forNode
}

func (r *Reader) IsMaterialized() bool {
	return r.state != nil
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
		st.Add(dataToAdd, r.columnsForView)
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

func (r *Reader) Parameters() []ViewParameter {
	return r.parameters
}

func (r *Reader) PublicColumnLength() int {
	return r.columnsForUser
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

func NewReader(forNode graph.NodeIdx) *Reader {
	return &Reader{forNode: forNode}
}

func (r *Reader) ToProto() *flownodepb.Node_Reader {
	return &flownodepb.Node_Reader{
		PublicId:       r.publicID,
		ForNode:        r.forNode,
		State:          r.state,
		Parameters:     r.parameters,
		ColumnsForView: r.columnsForView,
		ColumnsForUser: r.columnsForUser,
		TopkOrder:      r.topkOrder,
		TopkLimit:      r.topkLimit,
	}
}

func (r *Reader) SetPublicID(id string) {
	r.publicID = id
}

func (r *Reader) PublicID() string {
	return r.publicID
}

func NewReaderFromProto(r *flownodepb.Node_Reader) *Reader {
	return &Reader{
		writer:         nil,
		publicID:       r.PublicId,
		forNode:        r.ForNode,
		state:          r.State,
		parameters:     r.Parameters,
		columnsForView: r.ColumnsForView,
		columnsForUser: r.ColumnsForUser,
		topkOrder:      r.TopkOrder,
		topkLimit:      r.TopkLimit,
	}
}

package flownode

import (
	"context"
	"math/rand"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/common"
	"vitess.io/vitess/go/boost/dataflow/backlog"
	"vitess.io/vitess/go/boost/dataflow/trace"
	"vitess.io/vitess/go/boost/graph"
)

var _ NodeImpl = (*Reader)(nil)

type Reader struct {
	// not serialized
	writer *backlog.Writer

	forNode    graph.NodeIdx
	state      []int
	parameters []boostpb.ViewParameter

	topkOrder Order
	topkLimit int

	columnsForView int
	columnsForUser int
}

func (r *Reader) dataflow() {}

func (r *Reader) Key() []int {
	return r.state
}

func (r *Reader) OnConnected(ingredients *graph.Graph[*Node], key []int, parameters []boostpb.ViewParameter, colLen int) {
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

func (r *Reader) Order() (cols []int64, desc []bool, limit int64) {
	for _, ord := range r.topkOrder {
		cols = append(cols, int64(ord.Col))
		desc = append(desc, ord.Desc)
	}
	return cols, desc, int64(r.topkLimit)
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

func (r *Reader) SetWriteHandle(w *backlog.Writer) {
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

func (r *Reader) Process(ctx context.Context, m **boostpb.Packet, swap bool) error {
	if trace.T {
		var span *trace.Span
		ctx, span = trace.WithSpan(ctx, "Reader.Process", *m)
		defer span.Close()
	}

	if st := r.writer; st != nil {
		m := *m

		isRegular := m.GetMessage() != nil
		if isRegular && st.IsPartial() {
			m.MapData(func(records *[]boostpb.Record) {
				data := (*records)[:0]
				for _, row := range *records {
					if st.EntryFromRecord(row.Row).Found() {
						// state is already present,
						// so we can safely keep it up to date.
						data = append(data, row)
						// Otherwise row would miss in partial state.
						// leave it blank so later lookup triggers replay.
					}
				}
				*records = data
			})
		}

		// it *can* happen that multiple readers miss (and thus request replay for) the
		// same hole at the same time. we need to make sure that we ignore any such
		// duplicated replay.
		if !isRegular && st.IsPartial() {
			m.MapData(func(records *[]boostpb.Record) {
				data := (*records)[:0]
				for _, row := range *records {
					if st.EntryFromRecord(row.Row).Found() {
						// a given key should only be replayed to once!
					} else {
						// filling a hole with replay -- ok
						data = append(data, row)
					}
				}
				*records = data
			})
		}

		dataToAdd := m.TakeData()
		if trace.T {
			trace.GetSpan(ctx).Arg("added", dataToAdd)
		}

		st.Add(dataToAdd, r.columnsForView)
		if swap {
			st.Swap()
		}
	}
	return nil
}

func (r *Reader) Writer() *backlog.Writer {
	return r.writer
}

func (r *Reader) Shard(factor uint) {
	// noop
}

func (r *Reader) OnEviction(keys []boostpb.Row) {
	// NOTE: *could* be None if reader has been created but its state hasn't been built yet
	if w := r.writer; w != nil {
		for _, k := range keys {
			w.WithKey(k).MarkHole()
		}
		w.Swap()
	}
}

func (r *Reader) Parameters() []boostpb.ViewParameter {
	return r.parameters
}

func (r *Reader) PublicColumnLength() int {
	return r.columnsForUser
}

func (r *Reader) KeySchema() []boostpb.Type {
	return r.writer.KeySchema()
}

func (r *Reader) StateSizeAtomic() *common.AtomicInt64 {
	return r.writer.StateSizeAtomic()
}

func (r *Reader) EvictRandomKeys(rng *rand.Rand, bytesToEvict int64) {
	r.writer.EvictRandomKeys(rng, bytesToEvict)
	r.writer.Swap()
}

func NewReader(forNode graph.NodeIdx) *Reader {
	return &Reader{forNode: forNode}
}

func (r *Reader) ToProto() *boostpb.Node_Reader {
	return &boostpb.Node_Reader{
		ForNode:        r.forNode,
		State:          r.state,
		Parameters:     r.parameters,
		ColumnsForView: r.columnsForView,
		ColumnsForUser: r.columnsForUser,
		TopkOrder:      r.topkOrder,
		TopkLimit:      r.topkLimit,
	}
}

func NewReaderFromProto(r *boostpb.Node_Reader) *Reader {
	return &Reader{
		writer:         nil,
		forNode:        r.ForNode,
		state:          r.State,
		parameters:     r.Parameters,
		columnsForView: r.ColumnsForView,
		columnsForUser: r.ColumnsForUser,
		topkOrder:      r.TopkOrder,
		topkLimit:      r.TopkLimit,
	}
}

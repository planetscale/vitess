package flownode

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/dataflow/domain/replay"
	"vitess.io/vitess/go/boost/dataflow/flownode/flownodepb"
	"vitess.io/vitess/go/boost/dataflow/processing"
	"vitess.io/vitess/go/boost/dataflow/state"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/server/controller/boostplan/viewplan"
	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/vt/vthash"
)

type OrderedColumn = viewplan.OrderedColumn
type Order []OrderedColumn

func (ord Order) Cmp(a, b sql.Row) int {
	for _, oc := range ord {
		if cmp := a.ValueAt(oc.Col).Cmp(b.ValueAt(oc.Col)); cmp != 0 {
			if oc.Desc {
				return -cmp
			}
			return cmp
		}
	}
	return 0
}

var _ Internal = (*TopK)(nil)

type TopK struct {
	src       dataflow.IndexPair
	srcSchema []sql.Type

	keys  []int
	order Order
	k     int
}

func (t *TopK) internal() {}

func (t *TopK) dataflow() {}

func NewTopK(src graph.NodeIdx, order []OrderedColumn, keys []int, k int) *TopK {
	sort.Ints(keys)
	return &TopK{
		src:   dataflow.NewIndexPair(src),
		keys:  keys,
		order: order,
		k:     k,
	}
}

func (t *TopK) DataflowNode() {}

func (t *TopK) Ancestors() []graph.NodeIdx {
	return []graph.NodeIdx{t.src.AsGlobal()}
}

func (t *TopK) SuggestIndexes(you graph.NodeIdx) map[graph.NodeIdx][]int {
	return map[graph.NodeIdx][]int{you: t.keys}
}

func (t *TopK) Resolve(col int) []NodeColumn {
	return []NodeColumn{{Node: t.src.AsGlobal(), Column: col}}
}

func (t *TopK) ParentColumns(col int) []NodeColumn {
	return []NodeColumn{{Node: t.src.AsGlobal(), Column: col}}
}

func (t *TopK) ColumnType(g *graph.Graph[*Node], col int) (sql.Type, error) {
	return g.Value(t.src.AsGlobal()).ColumnType(g, col)
}

func (t *TopK) Description() string {
	var order []string
	for _, o := range t.order {
		orderName := "ASC"
		if o.Desc {
			orderName = "DESC"
		}
		order = append(order, fmt.Sprintf(":%d %s", o.Col, orderName))
	}
	var params []string
	for _, key := range t.keys {
		params = append(params, fmt.Sprintf("%d", key))
	}
	return fmt.Sprintf("ORDER BY %s LIMIT %d PARAMS %s", strings.Join(order, ", "), t.k, strings.Join(params, ", "))
}

func (t *TopK) OnConnected(graph *graph.Graph[*Node]) error {
	var err error
	t.srcSchema, err = graph.Value(t.src.AsGlobal()).ResolveSchema(graph)
	return err
}

func (t *TopK) OnCommit(you graph.NodeIdx, remap map[graph.NodeIdx]dataflow.IndexPair) {
	t.src.Remap(remap)
}

func (t *TopK) OnInput(you *Node, ex processing.Executor, _ dataflow.LocalNodeIdx, rs []sql.Record, repl replay.Context, _ *Map, states *state.Map) (processing.Result, error) {
	if len(rs) == 0 {
		return processing.Result{Records: rs}, nil
	}

	groupBy := t.keys
	hashrs := make([]sql.HashedRecord, 0, len(rs))

	var hasher vthash.Hasher
	for _, r := range rs {
		hashrs = append(hashrs, sql.HashedRecord{
			Record: r,
			Hash:   r.Row.HashWithKey(&hasher, groupBy, t.srcSchema),
		})
	}
	slices.SortStableFunc(hashrs, func(a, b sql.HashedRecord) int {
		return bytes.Compare(a.Hash[:], b.Hash[:])
	})

	us := you.index
	db := states.Get(us.AsLocal())
	if db == nil {
		panic("topk operators must have their state materialized")
	}

	type newrow struct {
		row sql.Row
		new bool
	}

	var (
		out     []sql.Record
		grp     sql.Row
		grpHash vthash.Hash
		grpk    int
		missed  bool
		flush   bool
		current []newrow
		misses  []processing.Miss
		lookups []processing.Lookup
	)

	postGroup := func(out []sql.Record, current []newrow, grpk, k int, order Order) ([]sql.Record, bool) {
		slices.SortFunc(current, func(a, b newrow) int {
			return order.Cmp(a.row, b.row)
		})

		if k < 0 {
			for i := 0; i < len(current); i++ {
				if current[i].new {
					out = append(out, current[i].row.ToRecord(true))
				}
			}
			return out, false
		}

		if grpk == k {
			if len(current) < grpk {
				return nil, true
			}

			// FIXME: if all the elements with the smallest value in the new topk are new,
			// then it *could* be that there exists some value that is greater than all
			// those values, and <= the smallest old value. we would only discover that by
			// querying. unfortunately, the check below isn't *quite* right because it does
			// not consider old rows that were removed in this batch (which should still be
			// counted for this condition).
			/*
				let all_new_bottom = $current[start..]
					.iter()
					.take_while(|(ref r, _)| {
						$order.cmp(r, &$current[start].0) == Ordering::Equal
					})
					.all(|&(_, is_new)| is_new);
				if all_new_bottom {
					eprintln!("topk is guesstimating bottom row");
				}
			*/
		}

		// optimization: if we don't *have to* remove something, we don't
		for i := k; i < len(current); i++ {
			cur := &current[i]
			if cur.new {
				// we found an `is_new` in current
				// can we replace it with a !is_new with the same order value?
				replace := slices.IndexFunc(current[k:], func(r newrow) bool {
					return !r.new && order.Cmp(r.row, cur.row) == 0
				})
				if replace >= 0 {
					current[i], current[replace] = current[replace], current[i]
				}
			}
		}

		for i := 0; i < k && i < len(current); i++ {
			if current[i].new {
				out = append(out, current[i].row.ToRecord(true))
			}
		}

		for i := k; i < len(current); i++ {
			if !current[i].new {
				out = append(out, current[i].row.ToRecord(false))
			}
		}

		return out, false
	}

	replKey := repl.Key()
	for _, r := range hashrs {
		if r.Hash != grpHash {
			if len(grp) > 0 {
				out, flush = postGroup(out, current, grpk, t.k, t.order)
				current = current[:0]

				if flush {
					goto handleRow
				}
			}

			grp = r.Row.Extract(groupBy)
			grpHash = r.Hash
			flush = false

			lookup, found := db.Lookup(groupBy, grp)
			if !found {
				missed = true
			} else {
				if replKey != nil {
					lookups = append(lookups, processing.Lookup{
						On:   us.Local,
						Cols: groupBy,
						Key:  grp,
					})
				}

				missed = false
				grpk = lookup.Len()
				lookup.ForEach(func(r sql.Row) {
					current = append(current, newrow{row: r, new: false})
				})
			}
		}

	handleRow:
		if missed || flush {
			misses = append(misses, processing.Miss{
				On:         us.Local,
				LookupIdx:  groupBy,
				LookupCols: groupBy,
				ReplayCols: replKey,
				Record:     r,
				Flush:      flush,
				ForceTag:   dataflow.TagNone,
			})
		} else {
			if r.Positive {
				current = append(current, newrow{row: r.Row, new: true})
			} else {
				p := slices.IndexFunc(current, func(x newrow) bool {
					return r.Row == x.row
				})
				if p >= 0 {
					if !current[p].new {
						out = append(out, r.Row.ToRecord(false))
					}
					current[p] = current[len(current)-1]
					current = current[:len(current)-1]
				}
			}
		}
	}
	if len(grp) > 0 {
		out, flush = postGroup(out, current, grpk, t.k, t.order)
		if flush {
			misses = append(misses, processing.Miss{
				On:         us.Local,
				LookupIdx:  groupBy,
				LookupCols: groupBy,
				ReplayCols: replKey,
				Record:     hashrs[len(hashrs)-1],
				Flush:      flush,
				ForceTag:   dataflow.TagNone,
			})
		}
	}
	return processing.Result{
		Records: out,
		Misses:  misses,
		Lookups: lookups,
	}, nil
}

func (t *TopK) ToProto() *flownodepb.Node_InternalTopK {
	pb := &flownodepb.Node_InternalTopK{
		Src:       t.src,
		SrcSchema: t.srcSchema,
		K:         t.k,
		Params:    t.keys,
		Order:     t.order,
	}
	return pb
}

func NewTopKFromProto(pb *flownodepb.Node_InternalTopK) *TopK {
	return &TopK{
		src:       pb.Src,
		srcSchema: pb.SrcSchema,
		keys:      pb.Params,
		order:     pb.Order,
		k:         pb.K,
	}
}

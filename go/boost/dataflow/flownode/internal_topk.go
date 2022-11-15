package flownode

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/dataflow/processing"
	"vitess.io/vitess/go/boost/dataflow/state"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/vt/vthash"
)

type Order []boostpb.OrderedColumn

func (ord Order) Cmp(a, b boostpb.Row) int {
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
	src  boostpb.IndexPair
	cols int

	keys  []int
	order Order
	k     uint
}

func (t *TopK) internal() {}

func (t *TopK) dataflow() {}

func NewTopK(src graph.NodeIdx, order []boostpb.OrderedColumn, keys []int, k uint) *TopK {
	sort.Ints(keys)
	return &TopK{
		src:   boostpb.NewIndexPair(src),
		cols:  0,
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
	return []NodeColumn{{t.src.AsGlobal(), col}}
}

func (t *TopK) ParentColumns(col int) []NodeColumn {
	return []NodeColumn{{t.src.AsGlobal(), col}}
}

func (t *TopK) ColumnType(g *graph.Graph[*Node], col int) boostpb.Type {
	return g.Value(t.src.AsGlobal()).ColumnType(g, col)
}

func (t *TopK) Description(detailed bool) string {
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
	return fmt.Sprintf("order by %s limit %d params %s", strings.Join(order, ", "), t.k, strings.Join(params, ", "))
}

func (t *TopK) OnConnected(graph *graph.Graph[*Node]) {
	srcn := graph.Value(t.src.AsGlobal())
	t.cols = len(srcn.Fields())
}

func (t *TopK) OnCommit(you graph.NodeIdx, remap map[graph.NodeIdx]boostpb.IndexPair) {
	t.src.Remap(remap)
}

func (t *TopK) OnInput(you *Node, ex processing.Executor, from boostpb.LocalNodeIndex, rs []boostpb.Record, replayKeyCol []int, domain *Map, states *state.Map) (processing.Result, error) {
	if len(rs) == 0 {
		return processing.Result{Records: rs}, nil
	}

	groupBy := t.keys
	pschema := domain.Get(from).schema
	hashrs := make([]boostpb.HashedRecord, 0, len(rs))

	var hasher vthash.Hasher
	for _, r := range rs {
		hashrs = append(hashrs, boostpb.HashedRecord{
			Record: r,
			Hash:   r.Row.HashWithKey(&hasher, groupBy, pschema),
		})
	}
	slices.SortStableFunc(hashrs, func(a, b boostpb.HashedRecord) bool {
		return bytes.Compare(a.Hash[:], b.Hash[:]) < 0
	})

	us := you.index
	db := states.Get(us.AsLocal())
	if db == nil {
		panic("topk operators must have their state materialized")
	}

	type newrow struct {
		row boostpb.Row
		new bool
	}

	var (
		out     []boostpb.Record
		grp     boostpb.Row
		grpHash vthash.Hash
		grpk    uint
		missed  bool
		current []newrow
		misses  []processing.Miss
		lookups []processing.Lookup
	)

	postGroup := func(out []boostpb.Record, current []newrow, grpk, k uint, order Order) []boostpb.Record {
		slices.SortFunc(current, func(a, b newrow) bool {
			return order.Cmp(a.row, b.row) < 0
		})

		if grpk == k {
			if len(current) < int(grpk) {
				// there used to be k things in the group
				// now there are fewer than k
				// we don't know if querying would bring us back to k
				panic("unimplemented")
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
		for i := int(k); i < len(current); i++ {
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

		for i := 0; i < int(k) && i < len(current); i++ {
			if current[i].new {
				out = append(out, current[i].row.ToRecord(true))
			}
		}

		for i := int(k); i < len(current); i++ {
			if !current[i].new {
				out = append(out, current[i].row.ToRecord(false))
			}
		}

		return out
	}

	for _, r := range hashrs {
		if r.Hash != grpHash {
			if len(grp) > 0 {
				out = postGroup(out, current, grpk, t.k, t.order)
				current = current[:0]
			}

			grp = r.Row.Extract(groupBy)
			grpHash = r.Hash

			lookup, found := db.Lookup(groupBy, grp)
			if !found {
				missed = true
			} else {
				if replayKeyCol != nil {
					lookups = append(lookups, processing.Lookup{
						On:   us.Local,
						Cols: groupBy,
						Key:  grp,
					})
				}

				missed = false
				grpk = uint(lookup.Len())
				lookup.ForEach(func(r boostpb.Row) {
					current = append(current, newrow{row: r, new: false})
				})
			}
		}

		if missed {
			misses = append(misses, processing.Miss{
				On:         us.Local,
				LookupIdx:  groupBy,
				LookupCols: groupBy,
				ReplayCols: replayKeyCol,
				Record:     r,
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
		out = postGroup(out, current, grpk, t.k, t.order)
	}
	return processing.Result{
		Records: out,
		Misses:  misses,
		Lookups: lookups,
	}, nil
}

func (t *TopK) ToProto() *boostpb.Node_InternalTopK {
	pb := &boostpb.Node_InternalTopK{
		Src:    &t.src,
		K:      t.k,
		Params: t.keys,
		Order:  t.order,
	}
	return pb
}

func NewTopKFromProto(pb *boostpb.Node_InternalTopK) *TopK {
	return &TopK{
		src:   *pb.Src,
		keys:  pb.Params,
		order: pb.Order,
		k:     pb.K,
	}
}

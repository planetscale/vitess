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
	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/vt/vthash"
)

type Distinct struct {
	src       dataflow.IndexPair
	srcSchema []sql.Type
	params    []int
}

func (d *Distinct) internal() {}

func (d *Distinct) dataflow() {}

func NewDistinct(src graph.NodeIdx, groupBy []int) *Distinct {
	sort.Ints(groupBy)
	return &Distinct{
		src:    dataflow.NewIndexPair(src),
		params: groupBy,
	}
}

func (d *Distinct) Ancestors() []graph.NodeIdx {
	return []graph.NodeIdx{d.src.AsGlobal()}
}

func (d *Distinct) SuggestIndexes(you graph.NodeIdx) map[graph.NodeIdx][]int {
	return map[graph.NodeIdx][]int{you: d.params}
}

func (d *Distinct) Resolve(col int) []NodeColumn {
	return []NodeColumn{{d.src.AsGlobal(), col}}
}

func (d *Distinct) ParentColumns(col int) []NodeColumn {
	return []NodeColumn{{Node: d.src.AsGlobal(), Column: col}}
}

func (d *Distinct) ColumnType(g *graph.Graph[*Node], col int) (sql.Type, error) {
	return g.Value(d.src.AsGlobal()).ColumnType(g, col)
}

func (d *Distinct) Description() string {
	var params []string
	for _, key := range d.params {
		params = append(params, fmt.Sprintf("%d", key))
	}
	return fmt.Sprintf("Distinct (%s)", strings.Join(params, ", "))
}

func (d *Distinct) OnConnected(graph *graph.Graph[*Node]) error {
	var err error
	d.srcSchema, err = graph.Value(d.src.AsGlobal()).ResolveSchema(graph)
	return err
}

func (d *Distinct) OnCommit(you graph.NodeIdx, remap map[graph.NodeIdx]dataflow.IndexPair) {
	d.src.Remap(remap)
}

func (d *Distinct) OnInput(you *Node, _ processing.Executor, _ dataflow.LocalNodeIdx, rs []sql.Record, _ replay.Context, domain *Map, states *state.Map) (processing.Result, error) {
	if len(rs) == 0 {
		return processing.Result{Records: rs}, nil
	}

	groupBy := d.params
	db := states.Get(you.index.Local)
	if db == nil {
		panic("Distinct must have its own state materialized")
	}

	var hashrs = make([]sql.HashedRecord, 0, len(rs))
	var hasher vthash.Hasher
	for _, r := range rs {
		if !r.Positive {
			hashrs = append(hashrs, sql.HashedRecord{
				Record: r,
				Hash:   r.Row.HashWithKey(&hasher, groupBy, d.srcSchema),
			})
		}
	}
	for _, r := range rs {
		if r.Positive {
			hashrs = append(hashrs, sql.HashedRecord{
				Record: r,
				Hash:   r.Row.HashWithKey(&hasher, groupBy, d.srcSchema),
			})
		}
	}
	slices.SortStableFunc(hashrs, func(a, b sql.HashedRecord) bool {
		return bytes.Compare(a.Hash[:], b.Hash[:]) < 0
	})

	var output = rs[:0]
	var prev sql.HashedRecord

	for _, rec := range hashrs {
		if rec.Hash == prev.Hash && prev.Positive == rec.Positive {
			continue
		}

		prev.Row = rec.Row.Extract(groupBy)
		prev.Hash = rec.Hash
		prev.Positive = rec.Positive

		lookup, found := db.Lookup(groupBy, prev.Row)
		if !found {
			panic("Distinct does not support partial materialization")
		}

		if rec.Positive {
			if lookup.Len() == 0 {
				output = append(output, rec.Record)
			}
		} else if lookup.Len() > 0 {
			output = append(output, rec.Record)
		}
	}

	return processing.Result{Records: output}, nil
}

func (d *Distinct) ToProto() *flownodepb.Node_InternalDistinct {
	return &flownodepb.Node_InternalDistinct{
		Src:       d.src,
		SrcSchema: d.srcSchema,
		Params:    d.params,
	}
}

func NewDistinctFromProto(distinct *flownodepb.Node_InternalDistinct) *Distinct {
	return &Distinct{
		src:       distinct.Src,
		srcSchema: distinct.SrcSchema,
		params:    distinct.Params,
	}
}

var _ Internal = (*Distinct)(nil)

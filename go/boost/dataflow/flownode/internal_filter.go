package flownode

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/dataflow/domain/replay"
	"vitess.io/vitess/go/boost/dataflow/flownode/flownodepb"
	"vitess.io/vitess/go/boost/dataflow/processing"
	"vitess.io/vitess/go/boost/dataflow/state"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

type Filter struct {
	src    dataflow.IndexPair
	filter []sql.EvalExpr
}

func (f *Filter) internal() {}

func (f *Filter) dataflow() {}

func (f *Filter) apply(env *evalengine.ExpressionEnv, r sql.Row) bool {
	env.Row = r.ToVitess()
	for _, f := range f.filter {
		res, err := env.Evaluate(f.Eval)
		if err != nil {
			panic(err)
		}
		if !res.ToBoolean() {
			return false
		}
	}
	return true
}

func (f *Filter) OnInput(_ *Node, _ processing.Executor, _ dataflow.LocalNodeIdx, rs []sql.Record, _ replay.Context, _ *Map, _ *state.Map) (processing.Result, error) {
	var newRecords = make([]sql.Record, 0, len(rs))
	var env evalengine.ExpressionEnv

	for _, r := range rs {
		if f.apply(&env, r.Row) {
			newRecords = append(newRecords, r)
		}
	}
	return processing.Result{
		Records: newRecords,
	}, nil
}

func (f *Filter) QueryThrough(columns []int, key sql.Row, nodes *Map, states *state.Map) (RowIterator, bool, MaterializedState) {
	rows, found, mat := nodeLookup(f.src.AsLocal(), columns, key, nodes, states)
	if !mat {
		return nil, false, NotMaterialized
	}
	if !found {
		return nil, false, IsMaterialized
	}

	var newRecords = make(RowSlice, 0, rows.Len())
	var env evalengine.ExpressionEnv
	rows.ForEach(func(r sql.Row) {
		if f.apply(&env, r) {
			newRecords = append(newRecords, r)
		}
	})

	return newRecords, true, IsMaterialized
}

var _ Internal = (*Filter)(nil)
var _ ingredientQueryThrough = (*Filter)(nil)

func (f *Filter) Ancestors() []graph.NodeIdx {
	return []graph.NodeIdx{f.src.AsGlobal()}
}

func (f *Filter) SuggestIndexes(you graph.NodeIdx) map[graph.NodeIdx][]int {
	return nil
}

func (f *Filter) Resolve(col int) []NodeColumn {
	return []NodeColumn{{Node: f.src.AsGlobal(), Column: col}}
}

func (f *Filter) ParentColumns(col int) []NodeColumn {
	return []NodeColumn{{Node: f.src.AsGlobal(), Column: col}}
}

func (f *Filter) ColumnType(g *graph.Graph[*Node], col int) (sql.Type, error) {
	return g.Value(f.src.AsGlobal()).ColumnType(g, col)
}

func (f *Filter) Description() string {
	var fs []string
	for _, f := range f.filter {
		fs = append(fs, sqlparser.String(f.Expr))
	}
	return "σ[" + strings.Join(fs, ", ") + "]"
}

func (f *Filter) OnConnected(graph *graph.Graph[*Node]) error {
	srcn := graph.Value(f.src.AsGlobal())
	if len(f.filter) > len(srcn.Fields()) {
		return fmt.Errorf("adjacent node might be a base with a suffix of removed columns: %v", f.filter)
	}
	return nil
}

func (f *Filter) OnCommit(_ graph.NodeIdx, remap map[graph.NodeIdx]dataflow.IndexPair) {
	f.src.Remap(remap)
}

func NewFilter(src graph.NodeIdx, filter []sql.EvalExpr) *Filter {
	return &Filter{
		src:    dataflow.NewIndexPair(src),
		filter: filter,
	}
}

func NewFilterFromProto(pbfilt *flownodepb.Node_InternalFilter) *Filter {
	return &Filter{
		src:    *pbfilt.Src,
		filter: sql.EvalExprsFromProto(pbfilt.Filter),
	}
}

func (f *Filter) ToProto() *flownodepb.Node_InternalFilter {
	return &flownodepb.Node_InternalFilter{
		Src:    &f.src,
		Filter: sql.EvalExprsToProto(f.filter),
	}
}

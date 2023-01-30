package flownode

import (
	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/sql"

	"vitess.io/vitess/go/boost/dataflow/domain/replay"
	"vitess.io/vitess/go/boost/dataflow/processing"
	"vitess.io/vitess/go/boost/dataflow/state"
	"vitess.io/vitess/go/boost/graph"
)

type NodeColumn struct {
	Node   graph.NodeIdx
	Column int
}

type Ingredient interface {
	Ancestors() []graph.NodeIdx
	SuggestIndexes(you graph.NodeIdx) map[graph.NodeIdx][]int
	Resolve(col int) []NodeColumn
	ParentColumns(col int) []NodeColumn
	ColumnType(g *graph.Graph[*Node], col int) (sql.Type, error)

	Description() string

	OnConnected(graph *graph.Graph[*Node]) error
	OnCommit(you graph.NodeIdx, remap map[graph.NodeIdx]dataflow.IndexPair)
	OnInput(you *Node, ex processing.Executor, from dataflow.LocalNodeIdx, rs []sql.Record, repl replay.Context, domain *Map, states *state.Map) (processing.Result, error)
}

type ingredientInputRaw interface {
	Ingredient
	OnInputRaw(ex processing.Executor, from dataflow.LocalNodeIdx, data []sql.Record, replay replay.Context, domain *Map, states *state.Map) (processing.RawResult, error)
}

type RowIterator interface {
	Len() int
	ForEach(func(row sql.Row))
}

type MaterializedState bool

const (
	IsMaterialized  MaterializedState = true
	NotMaterialized MaterializedState = false
)

type ingredientQueryThrough interface {
	Ingredient
	QueryThrough(columns []int, key sql.Row, nodes *Map, states *state.Map) (RowIterator, bool, MaterializedState)
}

type ingredientJoin interface {
	Ingredient
	isJoin()
	MustReplayAmong() map[graph.NodeIdx]struct{}
}

type ingredientEmptyState interface {
	Ingredient
	InitEmptyState(rs []sql.Record, repl replay.Context, st *state.Memory) []sql.Record
}

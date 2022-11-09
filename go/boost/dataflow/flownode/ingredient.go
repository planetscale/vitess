package flownode

import (
	"vitess.io/vitess/go/boost/boostpb"
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
	ColumnType(g *graph.Graph[*Node], col int) boostpb.Type

	Description(detailed bool) string

	OnConnected(graph *graph.Graph[*Node])
	OnCommit(you graph.NodeIdx, remap map[graph.NodeIdx]boostpb.IndexPair)
	OnInput(you *Node, ex processing.Executor, from boostpb.LocalNodeIndex, data []boostpb.Record, replayKeyCol []int, domain *Map, states *state.Map) (processing.Result, error)
}

type ingredientInputRaw interface {
	Ingredient
	OnInputRaw(ex processing.Executor, from boostpb.LocalNodeIndex, data []boostpb.Record, replay replay.Context, domain *Map, states *state.Map) (processing.RawResult, error)
}

type RowIterator interface {
	Len() int
	ForEach(func(row boostpb.Row))
}

type MaterializedState bool

const (
	IsMaterialized  MaterializedState = true
	NotMaterialized MaterializedState = false
)

type ingredientQueryThrough interface {
	Ingredient
	QueryThrough(columns []int, key boostpb.Row, nodes *Map, states *state.Map) (RowIterator, bool, MaterializedState)
}

type ingredientJoin interface {
	Ingredient
	isJoin()
	MustReplayAmong() map[graph.NodeIdx]struct{}
}

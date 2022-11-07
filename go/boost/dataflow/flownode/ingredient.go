package flownode

import (
	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/common/rowstore/offheap"
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

type IngredientQueryThrough interface {
	Ingredient
	QueryThrough() // TODO
}

type JoinIngredient interface {
	Ingredient
	IsJoin()
	MustReplayAmong() map[graph.NodeIdx]struct{}
}

func Lookup(ingredient Ingredient, parent boostpb.LocalNodeIndex, columns []int, key boostpb.Row, nodes *Map, states *state.Map) (*offheap.Rows, bool, bool, error) {
	parentState := states.Get(parent)
	if parentState == nil {
		parentNode := nodes.Get(parent)
		if !parentNode.IsInternal() {
			return nil, false, false, nil
		}
		_, ok := parentNode.impl.(IngredientQueryThrough)
		if !ok {
			return nil, false, false, nil
		}

		panic("unimplemented query trough")
	}

	rowBag, found := parentState.Lookup(columns, key)
	return rowBag, found, true, nil
}

package flownode

import (
	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/dataflow/processing"
	"vitess.io/vitess/go/boost/dataflow/state"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/sqltypes"
)

var _ Internal = (*Rewrite)(nil)

type Rewrite struct {
	src    boostpb.IndexPair
	signal boostpb.IndexPair

	rwCol     int
	value     sqltypes.Value
	signalKey int
}

func (r *Rewrite) internal() {}

func (r *Rewrite) dataflow() {}

func NewRewrite(src, signal graph.NodeIdx, rwCol int, value sqltypes.Value, signalKey int) *Rewrite {
	return &Rewrite{
		src:       boostpb.NewIndexPair(src),
		signal:    boostpb.NewIndexPair(signal),
		rwCol:     rwCol,
		value:     value,
		signalKey: signalKey,
	}
}

var _ JoinIngredient = (*Rewrite)(nil)

func (r *Rewrite) DataflowNode() {}

func (r *Rewrite) IsJoin() {}

func (r *Rewrite) MustReplayAmong() map[graph.NodeIdx]struct{} {
	//TODO implement me
	panic("implement me")
}

func (r *Rewrite) Ancestors() []graph.NodeIdx {
	//TODO implement me
	panic("implement me")
}

func (r *Rewrite) SuggestIndexes(you graph.NodeIdx) map[graph.NodeIdx][]int {
	//TODO implement me
	panic("implement me")
}

func (r *Rewrite) Resolve(col int) []NodeColumn {
	//TODO implement me
	panic("implement me")
}

func (r *Rewrite) ParentColumns(col int) []NodeColumn {
	//TODO implement me
	panic("implement me")
}

func (r *Rewrite) ColumnType(g *graph.Graph[*Node], col int) boostpb.Type {
	//TODO implement me
	panic("implement me")
}

func (r *Rewrite) Description(detailed bool) string {
	//TODO implement me
	panic("implement me")
}

func (r *Rewrite) OnConnected(graph *graph.Graph[*Node]) {
	//TODO implement me
	panic("implement me")
}

func (r *Rewrite) OnCommit(you graph.NodeIdx, remap map[graph.NodeIdx]boostpb.IndexPair) {
	//TODO implement me
	panic("implement me")
}

func (r *Rewrite) OnInput(you *Node, ex processing.Executor, from boostpb.LocalNodeIndex, data []boostpb.Record, replayKeyCol []int, domain *Map, states *state.Map) (processing.Result, error) {
	//TODO implement me
	panic("implement me")
}

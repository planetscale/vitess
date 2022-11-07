package flownode

import (
	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/dataflow/processing"
	"vitess.io/vitess/go/boost/dataflow/state"
	"vitess.io/vitess/go/boost/graph"
)

type Identity struct {
	src boostpb.IndexPair
}

func (i *Identity) internal() {}

func (i *Identity) dataflow() {}

func (i *Identity) OnInput(you *Node, ex processing.Executor, from boostpb.LocalNodeIndex, rs []boostpb.Record, replayKeyCol []int, domain *Map, states *state.Map) (processing.Result, error) {
	return processing.Result{
		Records: rs,
	}, nil
}

var _ Internal = (*Identity)(nil)

func (i *Identity) DataflowNode() {}

func (i *Identity) Ancestors() []graph.NodeIdx {
	return []graph.NodeIdx{i.src.AsGlobal()}
}

func (i *Identity) SuggestIndexes(you graph.NodeIdx) map[graph.NodeIdx][]int {
	return nil
}

func (i *Identity) Resolve(col int) []NodeColumn {
	return []NodeColumn{{i.src.AsGlobal(), col}}
}

func (i *Identity) ParentColumns(col int) []NodeColumn {
	return []NodeColumn{{i.src.AsGlobal(), col}}
}

func (i *Identity) ColumnType(g *graph.Graph[*Node], col int) boostpb.Type {
	return g.Value(i.src.AsGlobal()).ColumnType(g, col)
}

func (i *Identity) Description(_ bool) string {
	return "≡"
}

func (i *Identity) OnConnected(_ *graph.Graph[*Node]) {
	// no op
}

func (i *Identity) OnCommit(_ graph.NodeIdx, remap map[graph.NodeIdx]boostpb.IndexPair) {
	i.src.Remap(remap)
}

func NewIdentity(src graph.NodeIdx) *Identity {
	return &Identity{src: boostpb.NewIndexPair(src)}
}

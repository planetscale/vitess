package flownode

import (
	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/dataflow/domain/replay"
	"vitess.io/vitess/go/boost/dataflow/processing"
	"vitess.io/vitess/go/boost/dataflow/state"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/sql"
)

type Identity struct {
	src dataflow.IndexPair
}

func (i *Identity) internal() {}

func (i *Identity) dataflow() {}

func (i *Identity) OnInput(you *Node, ex processing.Executor, from dataflow.LocalNodeIdx, rs []sql.Record, repl replay.Context, domain *Map, states *state.Map) (processing.Result, error) {
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
	return []NodeColumn{{Node: i.src.AsGlobal(), Column: col}}
}

func (i *Identity) ParentColumns(col int) []NodeColumn {
	return []NodeColumn{{Node: i.src.AsGlobal(), Column: col}}
}

func (i *Identity) ColumnType(g *graph.Graph[*Node], col int) (sql.Type, error) {
	return g.Value(i.src.AsGlobal()).ColumnType(g, col)
}

func (i *Identity) Description() string {
	return "≡"
}

func (i *Identity) OnConnected(_ *graph.Graph[*Node]) error {
	// no op
	return nil
}

func (i *Identity) OnCommit(_ graph.NodeIdx, remap map[graph.NodeIdx]dataflow.IndexPair) {
	i.src.Remap(remap)
}

func NewIdentity(src graph.NodeIdx) *Identity {
	return &Identity{src: dataflow.NewIndexPair(src)}
}

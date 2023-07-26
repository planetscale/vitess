package flownode

import (
	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/dataflow/flownode/flownodepb"
	"vitess.io/vitess/go/boost/dataflow/processing"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/sql"
)

type unionEmitAll struct {
	from     dataflow.IndexPair
	sharding dataflow.Sharding
}

func (u *unionEmitAll) OnInput(_ dataflow.LocalNodeIdx, rs []sql.Record) (processing.Result, error) {
	return processing.Result{
		Records: rs,
	}, nil
}

func (u *unionEmitAll) Ancestors() []graph.NodeIdx {
	return []graph.NodeIdx{u.from.AsGlobal()}
}

func (u *unionEmitAll) Resolve(col int) []NodeColumn {
	return []NodeColumn{{Node: u.from.AsGlobal(), Column: col}}
}

func (u *unionEmitAll) Description() string {
	return "⊍"
}

func (u *unionEmitAll) ParentColumns(col int) []NodeColumn {
	return []NodeColumn{{Node: u.from.AsGlobal(), Column: col}}
}

func (u *unionEmitAll) ColumnType(g *graph.Graph[*Node], col int) (sql.Type, error) {
	return g.Value(u.from.AsGlobal()).ColumnType(g, col)
}

func (u *unionEmitAll) OnCommit(remap map[graph.NodeIdx]dataflow.IndexPair) {
	u.from.Remap(remap)
}

func (u *unionEmitAll) OnConnected(graph *graph.Graph[*Node]) error {
	return nil
}

func (u *unionEmitAll) ToProto() *flownodepb.Node_InternalUnion_EmitAll {
	return &flownodepb.Node_InternalUnion_EmitAll{
		From:     &u.from,
		Sharding: &u.sharding,
	}
}

func newUnionEmitAllFromProto(emit *flownodepb.Node_InternalUnion_EmitAll) *unionEmitAll {
	return &unionEmitAll{
		from:     *emit.From,
		sharding: *emit.Sharding,
	}
}

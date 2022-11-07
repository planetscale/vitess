package flownode

import (
	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/dataflow/processing"
	"vitess.io/vitess/go/boost/graph"
)

type unionEmitAll struct {
	from     boostpb.IndexPair
	sharding boostpb.Sharding
}

func (u *unionEmitAll) OnInput(_ boostpb.LocalNodeIndex, rs []boostpb.Record) (processing.Result, error) {
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

func (u *unionEmitAll) Description(_ bool) string {
	return "⊍"
}

func (u *unionEmitAll) ParentColumns(col int) []NodeColumn {
	return []NodeColumn{{u.from.AsGlobal(), col}}
}

func (u *unionEmitAll) ColumnType(g *graph.Graph[*Node], col int) boostpb.Type {
	return g.Value(u.from.AsGlobal()).ColumnType(g, col)
}

func (u *unionEmitAll) OnCommit(remap map[graph.NodeIdx]boostpb.IndexPair) {
	u.from.Remap(remap)
}

func (u *unionEmitAll) OnConnected(graph *graph.Graph[*Node]) {}

func (u *unionEmitAll) ToProto() *boostpb.Node_InternalUnion_EmitAll {
	return &boostpb.Node_InternalUnion_EmitAll{
		From:     &u.from,
		Sharding: &u.sharding,
	}
}

func newUnionEmitAllFromProto(emit *boostpb.Node_InternalUnion_EmitAll) *unionEmitAll {
	return &unionEmitAll{
		from:     *emit.From,
		sharding: *emit.Sharding,
	}
}

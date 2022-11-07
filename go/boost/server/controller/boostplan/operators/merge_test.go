package operators

//
//import (
//	"testing"
//
//	"github.com/stretchr/testify/assert"
//	"github.com/stretchr/testify/require"
//
//	"vitess.io/vitess/go/tools/graphviz"
//)
//
//var (
//	t1 = &Table{
//		TableName: "t1",
//		Keyspace:  "ks",
//	}
//)
//
//func TestMerge(t *testing.T) {
//	t.Skip()
//	tests := []struct {
//		name      string
//		operators []*View
//		op        *View
//		res       []*View
//	}{
//		{
//			name: "Merge base tables",
//			operators: []*View{
//				{
//					Input: t1,
//				},
//			},
//			op: &View{
//				Input: &Filter{
//					Input:      clone(t1),
//					Predicates: convertToExpr("id > 5"),
//				},
//			},
//			res: []*View{
//				{
//					Input: t1,
//				},
//				{
//					Input: &Filter{
//						Input:      t1,
//						Predicates: convertToExpr("id > 5"),
//					},
//				},
//			},
//		},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			got, err := Merge(tt.operators, tt.op)
//			require.NoError(t, err)
//			graphExpected := convertToGraphViz(t, tt.res)
//			graphGot := convertToGraphViz(t, got)
//			assert.EqualValues(t, graphExpected.ProduceDOT(), graphGot.ProduceDOT())
//			if t.Failed() {
//				_ = graphExpected.Render()
//				_ = graphGot.Render()
//			}
//		})
//	}
//}
//
//func clone(table *Table) *Table {
//	return &Table{
//		TableID:   table.TableID,
//		Keyspace:  table.Keyspace,
//		TableName: table.TableName,
//	}
//}
//
//func convertToGraphViz(t *testing.T, views []*View) *graphviz.Graph {
//	var ops []Operator
//	for _, view := range views {
//		ops = append(ops, view)
//	}
//	graph, err := GraphViz(ops)
//	require.NoError(t, err)
//	return graph
//}

package materialization

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/graph"
)

func TestGraphKeys(t *testing.T) {
	bases := func() (*graph.Graph[*flownode.Node], graph.NodeIdx, graph.NodeIdx) {
		g := new(graph.Graph[*flownode.Node])
		src := g.AddNode(flownode.New("source", []string{"dummy"}, &flownode.Root{}))
		schema := sql.TestSchema(sqltypes.Int64, sqltypes.Int64)

		a := g.AddNode(flownode.New("a", []string{"a1", "a2"}, flownode.NewTable("", "a", []int{0}, schema)))
		g.AddEdge(src, a)

		b := g.AddNode(flownode.New("b", []string{"b1", "b2"}, flownode.NewTable("", "b", []int{0}, schema)))
		g.AddEdge(src, b)

		return g, a, b
	}

	noparent := func(_ graph.NodeIdx, _ []int, _ []graph.NodeIdx) graph.NodeIdx {
		return graph.InvalidNode
	}

	sortPaths := func(paths [][]PathElement) {
		sort.Slice(paths, func(i, j int) bool {
			return slices.CompareFunc(paths[i], paths[j], func(a, b PathElement) int {
				return int(a.Node) - int(b.Node)
			}) < 0
		})
	}

	t.Run("base trace", func(t *testing.T) {
		g, a, b := bases()

		assert.Equal(t,
			[][]PathElement{{{a, []int{0}}}},
			ProvenanceOf(g, a, []int{0}, noparent),
		)

		assert.Equal(t,
			[][]PathElement{{{b, []int{0}}}},
			ProvenanceOf(g, b, []int{0}, noparent),
		)

		assert.Equal(t,
			[][]PathElement{{{a, []int{0, 1}}}},
			ProvenanceOf(g, a, []int{0, 1}, noparent),
		)

		assert.Equal(t,
			[][]PathElement{{{a, []int{1, 0}}}},
			ProvenanceOf(g, a, []int{1, 0}, noparent),
		)
	})

	t.Run("internal passthrough", func(t *testing.T) {
		g, a, _ := bases()
		x := g.AddNode(flownode.New("x", []string{"x1", "x2"}, &flownode.Ingress{}))
		g.AddEdge(a, x)

		assert.Equal(t,
			[][]PathElement{{{x, []int{0}}, {a, []int{0}}}},
			ProvenanceOf(g, x, []int{0}, noparent),
		)

		assert.Equal(t,
			[][]PathElement{{{x, []int{0, 1}}, {a, []int{0, 1}}}},
			ProvenanceOf(g, x, []int{0, 1}, noparent),
		)
	})

	t.Run("column reordering", func(t *testing.T) {
		g, a, _ := bases()

		project := flownode.NewProject(a, []flownode.Projection{flownode.ProjectedCol(1), flownode.ProjectedCol(0)})
		x := g.AddNode(flownode.New("x", []string{"x2", "x1"}, project))
		g.AddEdge(a, x)

		assert.Equal(t,
			[][]PathElement{{{x, []int{0}}, {a, []int{1}}}},
			ProvenanceOf(g, x, []int{0}, noparent),
		)

		assert.Equal(t,
			[][]PathElement{{{x, []int{0, 1}}, {a, []int{1, 0}}}},
			ProvenanceOf(g, x, []int{0, 1}, noparent),
		)
	})

	t.Run("generated columns", func(t *testing.T) {
		g, a, _ := bases()
		literal, _ := flownode.ProjectedLiteralFromAST(sqlparser.NewFloatLiteral("3.14"))
		projections := []flownode.Projection{
			flownode.ProjectedCol(0),
			literal,
		}
		project := flownode.NewProject(a, projections)
		x := g.AddNode(flownode.New("x", []string{"x1", "foo"}, project))
		g.AddEdge(a, x)

		assert.Equal(t,
			[][]PathElement{{{x, []int{0}}, {a, []int{0}}}},
			ProvenanceOf(g, x, []int{0}, noparent),
		)
		assert.Equal(t,
			[][]PathElement{{{x, []int{1}}, {a, []int{-1}}}},
			ProvenanceOf(g, x, []int{1}, noparent),
		)
		assert.Equal(t,
			[][]PathElement{{{x, []int{0, 1}}, {a, []int{0, -1}}}},
			ProvenanceOf(g, x, []int{0, 1}, noparent),
		)
	})

	t.Run("union straight", func(t *testing.T) {
		g, a, b := bases()

		union := flownode.NewUnion([]flownode.EmitTuple{
			{Ip: dataflow.NewIndexPair(a), Columns: []int{0, 1}},
			{Ip: dataflow.NewIndexPair(b), Columns: []int{0, 1}},
		})
		x := g.AddNode(flownode.New("x", []string{"x1", "x2"}, union))
		g.AddEdge(a, x)
		g.AddEdge(b, x)

		paths := ProvenanceOf(g, x, []int{0}, noparent)
		sortPaths(paths)
		assert.Equal(t,
			[][]PathElement{
				{{x, []int{0}}, {a, []int{0}}},
				{{x, []int{0}}, {b, []int{0}}},
			},
			paths,
		)

		paths = ProvenanceOf(g, x, []int{0, 1}, noparent)
		sortPaths(paths)
		assert.Equal(t,
			[][]PathElement{
				{{x, []int{0, 1}}, {a, []int{0, 1}}},
				{{x, []int{0, 1}}, {b, []int{0, 1}}},
			},
			paths,
		)
	})

	t.Run("join all", func(t *testing.T) {
		g, a, b := bases()

		join := flownode.NewJoin(a, b, flownode.JoinTypeInner,
			[2]int{1, 0},
			[][2]int{{0, -1}, {1, 0}, {-1, 1}})
		x := g.AddNode(flownode.New("x", []string{"a1", "a2b1", "b2"}, join))
		g.AddEdge(a, x)
		g.AddEdge(b, x)

		paths := ProvenanceOf(g, x, []int{0}, noparent)
		sortPaths(paths)
		assert.Equal(t,
			[][]PathElement{
				{{x, []int{0}}, {a, []int{0}}},
				{{x, []int{0}}, {b, []int{-1}}},
			},
			paths,
		)

		paths = ProvenanceOf(g, x, []int{1}, noparent)
		sortPaths(paths)
		assert.Equal(t,
			[][]PathElement{
				{{x, []int{1}}, {a, []int{1}}},
				{{x, []int{1}}, {b, []int{0}}},
			},
			paths,
		)

		paths = ProvenanceOf(g, x, []int{2}, noparent)
		sortPaths(paths)
		assert.Equal(t,
			[][]PathElement{
				{{x, []int{2}}, {a, []int{-1}}},
				{{x, []int{2}}, {b, []int{1}}},
			},
			paths,
		)

		paths = ProvenanceOf(g, x, []int{0, 1}, noparent)
		sortPaths(paths)
		assert.Equal(t,
			[][]PathElement{
				{{x, []int{0, 1}}, {a, []int{0, 1}}},
				{{x, []int{0, 1}}, {b, []int{-1, 0}}},
			},
			paths,
		)

		paths = ProvenanceOf(g, x, []int{1, 2}, noparent)
		sortPaths(paths)
		assert.Equal(t,
			[][]PathElement{
				{{x, []int{1, 2}}, {a, []int{1, -1}}},
				{{x, []int{1, 2}}, {b, []int{0, 1}}},
			},
			paths,
		)
	})
}

package flownode

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/dataflow/state"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
)

func fakeFilter(t *testing.T, expr sqlparser.Expr, columns ...int) sql.EvalExpr {
	expr = sqlparser.CloneExpr(expr)
	expr = sqlparser.Rewrite(expr, func(cursor *sqlparser.Cursor) bool {
		switch col := cursor.Node().(type) {
		case *sqlparser.ColName:
			offset, err := strconv.Atoi(strings.TrimPrefix(col.Name.Lowered(), "_col"))
			if err != nil {
				t.Fatalf("unexpected column name: %q", col.Name.Lowered())
			}
			cursor.Replace(sqlparser.NewOffset(columns[offset], col))
			return false
		}
		return true
	}, nil).(sqlparser.Expr)

	e := sql.EvalExpr{Expr: expr}
	if err := e.Compile(nil); err != nil {
		t.Fatalf("evalengine failed to compile: %v", err)
	}
	return e
}

func TestFilter(t *testing.T) {
	setup := func(t *testing.T, materialized bool, filters []sql.EvalExpr) *MockGraph {
		if filters == nil {
			filters = []sql.EvalExpr{
				fakeFilter(t, &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualOp,
					Left:     sqlparser.NewColName("_col0"),
					Right:    sqlparser.NewStrLiteral("a"),
				}, 1),
			}
		}

		var fields = []string{"x", "y"}
		g := NewMockGraph(t)
		s := g.AddBase("source", fields, sql.TestSchema(sqltypes.Int64, sqltypes.VarChar))
		g.SetOp("filter", fields, NewFilter(s.AsGlobal(), filters), materialized)
		return g
	}

	t.Run("it forwards with no filters", func(t *testing.T) {
		g := setup(t, false, []sql.EvalExpr{})

		left := sql.TestRow(1, "a")
		assert.Equal(t, left.AsRecords(), g.NarrowOneRow(left, false))

		left = sql.TestRow(1, "b")
		assert.Equal(t, left.AsRecords(), g.NarrowOneRow(left, false))

		left = sql.TestRow(2, "a")
		assert.Equal(t, left.AsRecords(), g.NarrowOneRow(left, false))
	})

	t.Run("it forwards", func(t *testing.T) {
		g := setup(t, false, nil)

		left := sql.TestRow(1, "a")
		assert.Equal(t, left.AsRecords(), g.NarrowOneRow(left, false))

		left = sql.TestRow(1, "b")
		assert.Equal(t, []sql.Record{}, g.NarrowOneRow(left, false))

		left = sql.TestRow(2, "a")
		assert.Equal(t, left.AsRecords(), g.NarrowOneRow(left, false))
	})

	t.Run("it forwards with multiple filters", func(t *testing.T) {
		filters := []sql.EvalExpr{
			fakeFilter(t, &sqlparser.ComparisonExpr{
				Operator: sqlparser.EqualOp,
				Left:     sqlparser.NewColName("_col0"),
				Right:    sqlparser.NewIntLiteral("1"),
			}, 0),
			fakeFilter(t, &sqlparser.ComparisonExpr{
				Operator: sqlparser.EqualOp,
				Left:     sqlparser.NewColName("_col0"),
				Right:    sqlparser.NewStrLiteral("a"),
			}, 1),
		}
		g := setup(t, false, filters)

		left := sql.TestRow(1, "a")
		assert.Equal(t, left.AsRecords(), g.NarrowOneRow(left, false))

		left = sql.TestRow(1, "b")
		assert.Equal(t, []sql.Record{}, g.NarrowOneRow(left, false))

		left = sql.TestRow(2, "a")
		assert.Equal(t, []sql.Record{}, g.NarrowOneRow(left, false))

		left = sql.TestRow(2, "b")
		assert.Equal(t, []sql.Record{}, g.NarrowOneRow(left, false))
	})

	t.Run("it suggests indices", func(t *testing.T) {
		g := setup(t, false, nil)
		idx := g.Node().SuggestIndexes(1)
		assert.Len(t, idx, 0)
	})

	t.Run("it resolves", func(t *testing.T) {
		g := setup(t, false, nil)

		assert.Equal(t, g.Node().Resolve(0), []NodeColumn{{
			g.NarrowBaseID().AsGlobal(), 0,
		}})

		assert.Equal(t, g.Node().Resolve(1), []NodeColumn{{
			g.NarrowBaseID().AsGlobal(), 1,
		}})
	})

	t.Run("it works with many", func(t *testing.T) {
		g := setup(t, false, nil)

		var many []sql.Record
		for i := 0; i < 10; i++ {
			many = append(many, sql.TestRow(int64(i), "a").AsRecord())
		}

		rs := g.NarrowOne(many, false)
		assert.Equal(t, many, rs, many)
	})

	t.Run("it works with inequalities", func(t *testing.T) {
		filters := []sql.EvalExpr{
			fakeFilter(t, &sqlparser.ComparisonExpr{
				Operator: sqlparser.LessEqualOp,
				Left:     sqlparser.NewColName("_col0"),
				Right:    sqlparser.NewIntLiteral("2"),
			}, 0),
			fakeFilter(t, &sqlparser.ComparisonExpr{
				Operator: sqlparser.NotEqualOp,
				Left:     sqlparser.NewColName("_col0"),
				Right:    sqlparser.NewStrLiteral("a"),
			}, 1),
		}
		g := setup(t, false, filters)

		left := sql.TestRow(2, "b")
		assert.Equal(t, left.AsRecords(), g.NarrowOneRow(left, false))

		left = sql.TestRow(2, "a")
		assert.Equal(t, []sql.Record{}, g.NarrowOneRow(left, false))

		left = sql.TestRow(3, "b")
		assert.Equal(t, []sql.Record{}, g.NarrowOneRow(left, false))

		left = sql.TestRow(1, "b")
		assert.Equal(t, left.AsRecords(), g.NarrowOneRow(left, false))
	})

	t.Run("it works with columns", func(t *testing.T) {
		filters := []sql.EvalExpr{
			fakeFilter(t, &sqlparser.ComparisonExpr{
				Operator: sqlparser.LessEqualOp,
				Left:     sqlparser.NewColName("_col0"),
				Right:    sqlparser.NewColName("_col1"),
			}, 0, 1),
		}
		g := setup(t, false, filters)

		left := sql.TestRow(2, 2)
		assert.Equal(t, left.AsRecords(), g.NarrowOneRow(left, false))

		left = sql.TestRow(2, "b")
		assert.Equal(t, []sql.Record{}, g.NarrowOneRow(left, false))
	})

	t.Run("it works with IN list", func(t *testing.T) {
		filters := []sql.EvalExpr{
			fakeFilter(t, &sqlparser.ComparisonExpr{
				Operator: sqlparser.InOp,
				Left:     sqlparser.NewColName("_col0"),
				Right: sqlparser.ValTuple{
					sqlparser.NewIntLiteral("2"),
					sqlparser.NewIntLiteral("42"),
				},
			}, 0),
			fakeFilter(t, &sqlparser.ComparisonExpr{
				Operator: sqlparser.InOp,
				Left:     sqlparser.NewColName("_col0"),
				Right: sqlparser.ValTuple{
					sqlparser.NewStrLiteral("b"),
				},
			}, 1),
		}
		g := setup(t, false, filters)

		left := sql.TestRow(2, "b")
		assert.Equal(t, left.AsRecords(), g.NarrowOneRow(left, false))

		left = sql.TestRow(2, "a")
		assert.Equal(t, []sql.Record{}, g.NarrowOneRow(left, false))

		left = sql.TestRow(3, "b")
		assert.Equal(t, []sql.Record{}, g.NarrowOneRow(left, false))

		left = sql.TestRow(42, "b")
		assert.Equal(t, left.AsRecords(), g.NarrowOneRow(left, false))
	})

	t.Run("it queries through", func(t *testing.T) {
		cond0 := fakeFilter(t, &sqlparser.ComparisonExpr{
			Operator: sqlparser.LessEqualOp,
			Left:     sqlparser.NewColName("_col0"),
			Right:    sqlparser.NewColName("_col1"),
		}, 0, 1)

		var cases = []struct {
			name     string
			filters  []sql.EvalExpr
			input    []sql.Row
			expected int
			column   int
			key      sql.Row
		}{
			{
				name:    "all",
				filters: []sql.EvalExpr{cond0},
				input: []sql.Row{
					sql.TestRow(1, 2, 3),
				},
				expected: 1,
				column:   0,
				key:      sql.TestRow(1),
			},
			{
				name:    "all but filtered",
				filters: []sql.EvalExpr{cond0},
				input: []sql.Row{
					sql.TestRow(2, 1, 3),
				},
				expected: 0,
				column:   0,
				key:      sql.TestRow(1),
			},
		}

		for _, tcase := range cases {
			t.Run(tcase.name, func(t *testing.T) {
				index := dataflow.IndexPair{
					Global: 0,
					Local:  0,
				}

				states := new(state.Map)
				schema := sql.TestSchema(sqltypes.Int64, sqltypes.Int64, sqltypes.Int64)

				st := state.NewMemoryState()
				st.AddKey([]int{0}, schema, nil, false)
				st.AddKey([]int{1}, schema, nil, false)

				var records []sql.Record
				for _, r := range tcase.input {
					records = append(records, r.ToRecord(true))
				}
				st.ProcessRecords(records, dataflow.TagNone, nil)
				states.Insert(0, st)

				project := NewFilter(0, tcase.filters)
				remap := map[graph.NodeIdx]dataflow.IndexPair{0: index}
				project.OnCommit(0, remap)

				iter, found, mat := project.QueryThrough([]int{tcase.column}, tcase.key, new(Map), states)
				require.Truef(t, bool(mat), "QueryThrough parent should be materialized")
				require.Truef(t, found, "QueryThrough should not miss")
				require.Equal(t, iter.Len(), tcase.expected)
			})
		}
	})
}

package flownode

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/dataflow/state"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

type forceColumn map[string]int

func (f forceColumn) ColumnLookup(col *sqlparser.ColName) (int, error) {
	offset, ok := f[col.Name.Lowered()]
	if !ok {
		return 0, fmt.Errorf("unexpected column name: %q", col.Name.Lowered())
	}
	return offset, nil
}

func (f forceColumn) CollationForExpr(_ sqlparser.Expr) collations.ID {
	return collations.CollationUtf8mb4ID
}

func (f forceColumn) DefaultCollation() collations.ID {
	return collations.CollationUtf8mb4ID
}

func fakeFilter(t *testing.T, expr sqlparser.Expr, columns ...int) FilterConditionTuple {
	var colmap = make(forceColumn)
	for n, c := range columns {
		colmap[fmt.Sprintf("_col%d", n)] = c
	}

	evalexpr, err := evalengine.Translate(expr, colmap)
	if err != nil {
		t.Fatalf("evalengine failed to translate: %v", err)
	}

	return FilterConditionTuple{ColumnID: columns[0], Cond: evalexpr}
}

func TestFilter(t *testing.T) {
	setup := func(t *testing.T, materialized bool, filters []FilterConditionTuple) *MockGraph {
		if filters == nil {
			filters = []FilterConditionTuple{
				fakeFilter(t, &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualOp,
					Left:     sqlparser.NewColName("_col0"),
					Right:    sqlparser.NewStrLiteral("a"),
				}, 1),
			}
		}

		var fields = []string{"x", "y"}
		g := NewMockGraph(t)
		s := g.AddBase("source", fields, boostpb.TestSchema(sqltypes.Int64, sqltypes.VarChar))
		g.SetOp("filter", fields, NewFilter(s.AsGlobal(), filters), materialized)
		return g
	}

	t.Run("it forwards with no filters", func(t *testing.T) {
		g := setup(t, false, []FilterConditionTuple{})

		left := boostpb.TestRow(1, "a")
		assert.Equal(t, left.AsRecords(), g.NarrowOneRow(left, false))

		left = boostpb.TestRow(1, "b")
		assert.Equal(t, left.AsRecords(), g.NarrowOneRow(left, false))

		left = boostpb.TestRow(2, "a")
		assert.Equal(t, left.AsRecords(), g.NarrowOneRow(left, false))
	})

	t.Run("it forwards", func(t *testing.T) {
		g := setup(t, false, nil)

		left := boostpb.TestRow(1, "a")
		assert.Equal(t, left.AsRecords(), g.NarrowOneRow(left, false))

		left = boostpb.TestRow(1, "b")
		assert.Equal(t, []boostpb.Record{}, g.NarrowOneRow(left, false))

		left = boostpb.TestRow(2, "a")
		assert.Equal(t, left.AsRecords(), g.NarrowOneRow(left, false))
	})

	t.Run("it forwards with multiple filters", func(t *testing.T) {
		filters := []FilterConditionTuple{
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

		left := boostpb.TestRow(1, "a")
		assert.Equal(t, left.AsRecords(), g.NarrowOneRow(left, false))

		left = boostpb.TestRow(1, "b")
		assert.Equal(t, []boostpb.Record{}, g.NarrowOneRow(left, false))

		left = boostpb.TestRow(2, "a")
		assert.Equal(t, []boostpb.Record{}, g.NarrowOneRow(left, false))

		left = boostpb.TestRow(2, "b")
		assert.Equal(t, []boostpb.Record{}, g.NarrowOneRow(left, false))
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

		var many []boostpb.Record
		for i := 0; i < 10; i++ {
			many = append(many, boostpb.TestRow(int64(i), "a").AsRecord())
		}

		rs := g.NarrowOne(many, false)
		assert.Equal(t, many, rs, many)
	})

	t.Run("it works with inequalities", func(t *testing.T) {
		filters := []FilterConditionTuple{
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

		left := boostpb.TestRow(2, "b")
		assert.Equal(t, left.AsRecords(), g.NarrowOneRow(left, false))

		left = boostpb.TestRow(2, "a")
		assert.Equal(t, []boostpb.Record{}, g.NarrowOneRow(left, false))

		left = boostpb.TestRow(3, "b")
		assert.Equal(t, []boostpb.Record{}, g.NarrowOneRow(left, false))

		left = boostpb.TestRow(1, "b")
		assert.Equal(t, left.AsRecords(), g.NarrowOneRow(left, false))
	})

	t.Run("it works with columns", func(t *testing.T) {
		filters := []FilterConditionTuple{
			fakeFilter(t, &sqlparser.ComparisonExpr{
				Operator: sqlparser.LessEqualOp,
				Left:     sqlparser.NewColName("_col0"),
				Right:    sqlparser.NewColName("_col1"),
			}, 0, 1),
		}
		g := setup(t, false, filters)

		left := boostpb.TestRow(2, 2)
		assert.Equal(t, left.AsRecords(), g.NarrowOneRow(left, false))

		left = boostpb.TestRow(2, "b")
		assert.Equal(t, []boostpb.Record{}, g.NarrowOneRow(left, false))
	})

	t.Run("it works with IN list", func(t *testing.T) {
		filters := []FilterConditionTuple{
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

		left := boostpb.TestRow(2, "b")
		assert.Equal(t, left.AsRecords(), g.NarrowOneRow(left, false))

		left = boostpb.TestRow(2, "a")
		assert.Equal(t, []boostpb.Record{}, g.NarrowOneRow(left, false))

		left = boostpb.TestRow(3, "b")
		assert.Equal(t, []boostpb.Record{}, g.NarrowOneRow(left, false))

		left = boostpb.TestRow(42, "b")
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
			filters  []FilterConditionTuple
			input    []boostpb.Row
			expected int
			column   int
			key      boostpb.Row
		}{
			{
				name:    "all",
				filters: []FilterConditionTuple{cond0},
				input: []boostpb.Row{
					boostpb.TestRow(1, 2, 3),
				},
				expected: 1,
				column:   0,
				key:      boostpb.TestRow(1),
			},
			{
				name:    "all but filtered",
				filters: []FilterConditionTuple{cond0},
				input: []boostpb.Row{
					boostpb.TestRow(2, 1, 3),
				},
				expected: 0,
				column:   0,
				key:      boostpb.TestRow(1),
			},
		}

		for _, tcase := range cases {
			t.Run(tcase.name, func(t *testing.T) {
				index := boostpb.IndexPair{
					Global: 0,
					Local:  0,
				}

				states := new(state.Map)
				schema := boostpb.TestSchema(sqltypes.Int64, sqltypes.Int64, sqltypes.Int64)

				st := state.NewMemoryState()
				st.AddKey([]int{0}, schema, nil)
				st.AddKey([]int{1}, schema, nil)

				var records []boostpb.Record
				for _, r := range tcase.input {
					records = append(records, r.ToRecord(true))
				}
				st.ProcessRecords(&records, boostpb.TagNone)
				states.Insert(0, st)

				project := NewFilter(0, tcase.filters)
				remap := map[graph.NodeIdx]boostpb.IndexPair{0: index}
				project.OnCommit(0, remap)

				iter, found, mat := project.QueryThrough([]int{tcase.column}, tcase.key, new(Map), states)
				require.Truef(t, bool(mat), "QueryThrough parent should be materialized")
				require.Truef(t, found, "QueryThrough should not miss")
				require.Equal(t, iter.Len(), tcase.expected)
			})
		}
	})
}

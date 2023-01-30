package flownode

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/sqltypes"
)

func TestGroupedAggregation(t *testing.T) {
	setup := func(t *testing.T, materialize bool) *MockGraph {
		g := NewMockGraph(t)
		s := g.AddBase("source", []string{"x", "y"}, sql.TestSchema(sqltypes.Int64, sqltypes.Int64))
		grp := NewGrouped(s.AsGlobal(), false, []int{0}, []Aggregation{{AggregationCount, 1}})

		g.SetOp("identity", []string{"x", "ys"}, grp, materialize)
		return g
	}

	setupMulticol := func(t *testing.T, materialize bool) *MockGraph {
		g := NewMockGraph(t)
		s := g.AddBase("source", []string{"x", "y", "z"}, sql.TestSchema(sqltypes.Int64, sqltypes.Int64, sqltypes.Int64))
		grp := NewGrouped(s.AsGlobal(), false, []int{0, 2}, []Aggregation{{AggregationCount, 1}})
		g.SetOp("identity", []string{"x", "z", "ys"}, grp, materialize)
		return g
	}

	t.Run("it forwards", func(t *testing.T) {
		c := setup(t, true)

		u := sql.TestRow(1, 1).ToRecord(true)
		rs := c.NarrowOne([]sql.Record{u}, true)
		assert.Len(t, rs, 1)
		assert.Equal(t, u, rs[0])

		u = sql.TestRow(2, 2).ToRecord(true)
		rs = c.NarrowOne([]sql.Record{u}, true)
		assert.Len(t, rs, 1)
		assert.Equal(t, true, rs[0].Positive)
		assert.Equal(t, sqltypes.NewInt64(2), rs[0].Row.ValueAt(0).ToVitess())
		assert.Equal(t, sqltypes.NewInt64(1), rs[0].Row.ValueAt(1).ToVitess())

		u = sql.TestRow(1, 2).ToRecord(true)
		rs = c.NarrowOne([]sql.Record{u}, true)
		assert.Len(t, rs, 2)
		assert.Equal(t, false, rs[0].Positive)
		assert.Equal(t, sqltypes.NewInt64(1), rs[0].Row.ValueAt(0).ToVitess())
		assert.Equal(t, sqltypes.NewInt64(1), rs[0].Row.ValueAt(1).ToVitess())
		assert.Equal(t, true, rs[1].Positive)
		assert.Equal(t, sqltypes.NewInt64(1), rs[1].Row.ValueAt(0).ToVitess())
		assert.Equal(t, sqltypes.NewInt64(2), rs[1].Row.ValueAt(1).ToVitess())

		uu := []sql.Record{
			sql.TestRow(1, 1).ToRecord(false),
		}
		rs = c.NarrowOne(uu, true)
		assert.Len(t, rs, 2)
		assert.Equal(t, false, rs[0].Positive)
		assert.Equal(t, sqltypes.NewInt64(1), rs[0].Row.ValueAt(0).ToVitess())
		assert.Equal(t, sqltypes.NewInt64(2), rs[0].Row.ValueAt(1).ToVitess())
		assert.Equal(t, true, rs[1].Positive)
		assert.Equal(t, sqltypes.NewInt64(1), rs[1].Row.ValueAt(0).ToVitess())
		assert.Equal(t, sqltypes.NewInt64(1), rs[1].Row.ValueAt(1).ToVitess())

		uu = []sql.Record{
			sql.TestRow(1, 1).ToRecord(false),
			sql.TestRow(1, 1).ToRecord(true),
			sql.TestRow(1, 2).ToRecord(true),
			sql.TestRow(2, 2).ToRecord(false),
			sql.TestRow(2, 2).ToRecord(true),
			sql.TestRow(2, 3).ToRecord(true),
			sql.TestRow(2, 1).ToRecord(true),
			sql.TestRow(3, 3).ToRecord(true),
		}
		rs = c.NarrowOne(uu, true)
		assert.Len(t, rs, 5)

		assert.Contains(t, rs, sql.TestRow(1, 1).ToRecord(false))
		assert.Contains(t, rs, sql.TestRow(1, 2).ToRecord(true))
		assert.Contains(t, rs, sql.TestRow(2, 1).ToRecord(false))
		assert.Contains(t, rs, sql.TestRow(2, 3).ToRecord(true))
		assert.Contains(t, rs, sql.TestRow(3, 1).ToRecord(true))
	})

	t.Run("it groups by multiple columns", func(t *testing.T) {
		c := setupMulticol(t, true)

		u := sql.TestRow(1, 1, 2).ToRecord(true)
		rs := c.NarrowOne([]sql.Record{u}, true)
		assert.Len(t, rs, 1)
		assert.Equal(t, true, rs[0].Positive)
		assert.Equal(t, sqltypes.NewInt64(1), rs[0].Row.ValueAt(0).ToVitess())
		assert.Equal(t, sqltypes.NewInt64(2), rs[0].Row.ValueAt(1).ToVitess())
		assert.Equal(t, sqltypes.NewInt64(1), rs[0].Row.ValueAt(2).ToVitess())

		u = sql.TestRow(2, 1, 2).ToRecord(true)
		rs = c.NarrowOne([]sql.Record{u}, true)
		assert.Len(t, rs, 1)
		assert.Equal(t, true, rs[0].Positive)
		assert.Equal(t, sqltypes.NewInt64(2), rs[0].Row.ValueAt(0).ToVitess())
		assert.Equal(t, sqltypes.NewInt64(2), rs[0].Row.ValueAt(1).ToVitess())
		assert.Equal(t, sqltypes.NewInt64(1), rs[0].Row.ValueAt(2).ToVitess())

		u = sql.TestRow(1, 1, 2).ToRecord(true)
		rs = c.NarrowOne([]sql.Record{u}, true)
		assert.Len(t, rs, 2)
		assert.Equal(t, false, rs[0].Positive)
		assert.Equal(t, sqltypes.NewInt64(1), rs[0].Row.ValueAt(0).ToVitess())
		assert.Equal(t, sqltypes.NewInt64(2), rs[0].Row.ValueAt(1).ToVitess())
		assert.Equal(t, sqltypes.NewInt64(1), rs[0].Row.ValueAt(2).ToVitess())
		assert.Equal(t, true, rs[1].Positive)
		assert.Equal(t, sqltypes.NewInt64(1), rs[1].Row.ValueAt(0).ToVitess())
		assert.Equal(t, sqltypes.NewInt64(2), rs[1].Row.ValueAt(1).ToVitess())
		assert.Equal(t, sqltypes.NewInt64(2), rs[1].Row.ValueAt(2).ToVitess())

		uu := []sql.Record{
			sql.TestRow(1, 1, 2).ToRecord(false),
		}
		rs = c.NarrowOne(uu, true)
		assert.Len(t, rs, 2)
		assert.Equal(t, false, rs[0].Positive)
		assert.Equal(t, sqltypes.NewInt64(1), rs[0].Row.ValueAt(0).ToVitess())
		assert.Equal(t, sqltypes.NewInt64(2), rs[0].Row.ValueAt(1).ToVitess())
		assert.Equal(t, sqltypes.NewInt64(2), rs[0].Row.ValueAt(2).ToVitess())
		assert.Equal(t, true, rs[1].Positive)
		assert.Equal(t, sqltypes.NewInt64(1), rs[1].Row.ValueAt(0).ToVitess())
		assert.Equal(t, sqltypes.NewInt64(2), rs[1].Row.ValueAt(1).ToVitess())
		assert.Equal(t, sqltypes.NewInt64(1), rs[1].Row.ValueAt(2).ToVitess())
	})

	t.Run("it suggests indices", func(t *testing.T) {
		c := setup(t, false)
		idx := c.Node().SuggestIndexes(1)

		assert.Len(t, idx, 1)
		assert.Equal(t, []int{0}, idx[1])
	})

	t.Run("it resolves", func(t *testing.T) {
		c := setup(t, false)
		assert.Equal(t, []NodeColumn{{c.NarrowBaseID().AsGlobal(), 0}}, c.Node().Resolve(0))
	})
}

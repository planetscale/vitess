package flownode

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/sqltypes"
)

func TestGroupedAggregation(t *testing.T) {
	setup := func(t *testing.T, materialize bool) *MockGraph {
		g := NewMockGraph(t)
		s := g.AddBase("source", []string{"x", "y"}, boostpb.TestSchema(sqltypes.Int64, sqltypes.Int64), nil)
		grp := NewGrouped(s.AsGlobal(), []int{0}, []AggrExpr{AggregationOver(AggregationCount, 1)})

		g.SetOp("identity", []string{"x", "ys"}, grp, materialize)
		return g
	}

	setupMulticol := func(t *testing.T, materialize bool) *MockGraph {
		g := NewMockGraph(t)
		s := g.AddBase("source", []string{"x", "y", "z"}, boostpb.TestSchema(sqltypes.Int64, sqltypes.Int64, sqltypes.Int64), nil)
		grp := NewGrouped(s.AsGlobal(), []int{0, 2}, []AggrExpr{AggregationOver(AggregationCount, 1)})
		g.SetOp("identity", []string{"x", "z", "ys"}, grp, materialize)
		return g
	}

	t.Run("it forwards", func(t *testing.T) {
		c := setup(t, true)

		u := boostpb.TestRow(1, 1).ToRecord(true)
		rs := c.NarrowOne([]boostpb.Record{u}, true)
		assert.Len(t, rs, 1)
		assert.Equal(t, u, rs[0])

		u = boostpb.TestRow(2, 2).ToRecord(true)
		rs = c.NarrowOne([]boostpb.Record{u}, true)
		assert.Len(t, rs, 1)
		assert.Equal(t, true, rs[0].Positive)
		assert.Equal(t, sqltypes.NewInt64(2), rs[0].Row.ValueAt(0).ToVitess())
		assert.Equal(t, sqltypes.NewInt64(1), rs[0].Row.ValueAt(1).ToVitess())

		u = boostpb.TestRow(1, 2).ToRecord(true)
		rs = c.NarrowOne([]boostpb.Record{u}, true)
		assert.Len(t, rs, 2)
		assert.Equal(t, false, rs[0].Positive)
		assert.Equal(t, sqltypes.NewInt64(1), rs[0].Row.ValueAt(0).ToVitess())
		assert.Equal(t, sqltypes.NewInt64(1), rs[0].Row.ValueAt(1).ToVitess())
		assert.Equal(t, true, rs[1].Positive)
		assert.Equal(t, sqltypes.NewInt64(1), rs[1].Row.ValueAt(0).ToVitess())
		assert.Equal(t, sqltypes.NewInt64(2), rs[1].Row.ValueAt(1).ToVitess())

		uu := []boostpb.Record{
			boostpb.TestRow(1, 1).ToRecord(false),
		}
		rs = c.NarrowOne(uu, true)
		assert.Len(t, rs, 2)
		assert.Equal(t, false, rs[0].Positive)
		assert.Equal(t, sqltypes.NewInt64(1), rs[0].Row.ValueAt(0).ToVitess())
		assert.Equal(t, sqltypes.NewInt64(2), rs[0].Row.ValueAt(1).ToVitess())
		assert.Equal(t, true, rs[1].Positive)
		assert.Equal(t, sqltypes.NewInt64(1), rs[1].Row.ValueAt(0).ToVitess())
		assert.Equal(t, sqltypes.NewInt64(1), rs[1].Row.ValueAt(1).ToVitess())

		uu = []boostpb.Record{
			boostpb.TestRow(1, 1).ToRecord(false),
			boostpb.TestRow(1, 1).ToRecord(true),
			boostpb.TestRow(1, 2).ToRecord(true),
			boostpb.TestRow(2, 2).ToRecord(false),
			boostpb.TestRow(2, 2).ToRecord(true),
			boostpb.TestRow(2, 3).ToRecord(true),
			boostpb.TestRow(2, 1).ToRecord(true),
			boostpb.TestRow(3, 3).ToRecord(true),
		}
		rs = c.NarrowOne(uu, true)
		assert.Len(t, rs, 5)

		assert.Contains(t, rs, boostpb.TestRow(1, 1).ToRecord(false))
		assert.Contains(t, rs, boostpb.TestRow(1, 2).ToRecord(true))
		assert.Contains(t, rs, boostpb.TestRow(2, 1).ToRecord(false))
		assert.Contains(t, rs, boostpb.TestRow(2, 3).ToRecord(true))
		assert.Contains(t, rs, boostpb.TestRow(3, 1).ToRecord(true))
	})

	t.Run("it groups by multiple columns", func(t *testing.T) {
		c := setupMulticol(t, true)

		u := boostpb.TestRow(1, 1, 2).ToRecord(true)
		rs := c.NarrowOne([]boostpb.Record{u}, true)
		assert.Len(t, rs, 1)
		assert.Equal(t, true, rs[0].Positive)
		assert.Equal(t, sqltypes.NewInt64(1), rs[0].Row.ValueAt(0).ToVitess())
		assert.Equal(t, sqltypes.NewInt64(2), rs[0].Row.ValueAt(1).ToVitess())
		assert.Equal(t, sqltypes.NewInt64(1), rs[0].Row.ValueAt(2).ToVitess())

		u = boostpb.TestRow(2, 1, 2).ToRecord(true)
		rs = c.NarrowOne([]boostpb.Record{u}, true)
		assert.Len(t, rs, 1)
		assert.Equal(t, true, rs[0].Positive)
		assert.Equal(t, sqltypes.NewInt64(2), rs[0].Row.ValueAt(0).ToVitess())
		assert.Equal(t, sqltypes.NewInt64(2), rs[0].Row.ValueAt(1).ToVitess())
		assert.Equal(t, sqltypes.NewInt64(1), rs[0].Row.ValueAt(2).ToVitess())

		u = boostpb.TestRow(1, 1, 2).ToRecord(true)
		rs = c.NarrowOne([]boostpb.Record{u}, true)
		assert.Len(t, rs, 2)
		assert.Equal(t, false, rs[0].Positive)
		assert.Equal(t, sqltypes.NewInt64(1), rs[0].Row.ValueAt(0).ToVitess())
		assert.Equal(t, sqltypes.NewInt64(2), rs[0].Row.ValueAt(1).ToVitess())
		assert.Equal(t, sqltypes.NewInt64(1), rs[0].Row.ValueAt(2).ToVitess())
		assert.Equal(t, true, rs[1].Positive)
		assert.Equal(t, sqltypes.NewInt64(1), rs[1].Row.ValueAt(0).ToVitess())
		assert.Equal(t, sqltypes.NewInt64(2), rs[1].Row.ValueAt(1).ToVitess())
		assert.Equal(t, sqltypes.NewInt64(2), rs[1].Row.ValueAt(2).ToVitess())

		uu := []boostpb.Record{
			boostpb.TestRow(1, 1, 2).ToRecord(false),
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

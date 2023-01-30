package flownode

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/sqltypes"
)

func TestGroupedExtremum(t *testing.T) {
	setup := func(t *testing.T, op AggregationKind, materialize bool) *MockGraph {
		g := NewMockGraph(t)
		s := g.AddBase("source", []string{"x", "y"}, sql.TestSchema(sqltypes.Int64, sqltypes.Int64))
		grp := NewGrouped(s.AsGlobal(), false, []int{0}, []Aggregation{{op, 1}})
		g.SetOp("agg", []string{"x", "ys"}, grp, materialize)
		return g
	}

	assertPositiveRecord := func(t *testing.T, group, new int64, rs []sql.Record) {
		require.Len(t, rs, 1)

		require.True(t, rs[0].Positive)
		require.Equal(t, sqltypes.NewInt64(group), rs[0].Row.ValueAt(0).ToVitessUnsafe())
		require.Equal(t, sqltypes.NewInt64(new), rs[0].Row.ValueAt(1).ToVitessUnsafe())
	}

	assertRecordChange := func(t *testing.T, group, old, new int64, rs []sql.Record) {
		require.Len(t, rs, 2)

		require.False(t, rs[0].Positive)
		require.Equal(t, sqltypes.NewInt64(group), rs[0].Row.ValueAt(0).ToVitessUnsafe())
		require.Equal(t, sqltypes.NewInt64(old), rs[0].Row.ValueAt(1).ToVitessUnsafe())

		require.True(t, rs[1].Positive)
		require.Equal(t, sqltypes.NewInt64(group), rs[1].Row.ValueAt(0).ToVitessUnsafe())
		require.Equal(t, sqltypes.NewInt64(new), rs[1].Row.ValueAt(1).ToVitessUnsafe())
	}

	t.Run("it forwards maximum", func(t *testing.T) {
		c := setup(t, ExtremumMax, true)
		key := int64(1)

		record := sql.TestRow(key, 4)
		out := c.NarrowOneRow(record, true)
		assertPositiveRecord(t, key, 4, out)

		record = sql.TestRow(key, 7)
		out = c.NarrowOneRow(record, true)
		assertRecordChange(t, key, 4, 7, out)

		record = sql.TestRow(key, 2)
		out = c.NarrowOneRow(record, true)
		require.Len(t, out, 0)

		record = sql.TestRow(2, 3)
		out = c.NarrowOneRow(record, true)
		assertPositiveRecord(t, 2, 3, out)

		record = sql.TestRow(key, 5)
		out = c.NarrowOneRow(record, true)
		require.Len(t, out, 0)

		record = sql.TestRow(key, 22)
		out = c.NarrowOneRow(record, true)
		assertRecordChange(t, key, 7, 22, out)

		u := []sql.Record{
			sql.TestRow(key, 22).ToRecord(false),
			sql.TestRow(key, 23).ToRecord(true),
		}
		out = c.NarrowOne(u, true)
		assertRecordChange(t, key, 22, 23, out)
	})

	t.Run("it forwards minimum", func(t *testing.T) {
		c := setup(t, ExtremumMin, true)
		key := int64(1)

		record := sql.TestRow(key, 10)
		out := c.NarrowOneRow(record, true)
		assertPositiveRecord(t, key, 10, out)

		record = sql.TestRow(key, 7)
		out = c.NarrowOneRow(record, true)
		assertRecordChange(t, key, 10, 7, out)

		record = sql.TestRow(key, 9)
		out = c.NarrowOneRow(record, true)
		require.Len(t, out, 0)

		record = sql.TestRow(2, 15)
		out = c.NarrowOneRow(record, true)
		assertPositiveRecord(t, 2, 15, out)

		record = sql.TestRow(key, 8)
		out = c.NarrowOneRow(record, true)
		require.Len(t, out, 0)

		u := []sql.Record{
			sql.TestRow(key, 7).ToRecord(false),
			sql.TestRow(key, 5).ToRecord(true),
		}
		out = c.NarrowOne(u, true)
		assertRecordChange(t, key, 7, 5, out)
	})

	t.Run("it cancels out opposite records", func(t *testing.T) {
		c := setup(t, ExtremumMax, true)

		c.NarrowOneRow(sql.TestRow(1, 5), true)
		u := []sql.Record{
			sql.TestRow(1, 10).ToRecord(true),
			sql.TestRow(1, 10).ToRecord(false),
		}
		out := c.NarrowOne(u, true)
		require.Len(t, out, 0)
	})

	t.Run("it suggests indices", func(t *testing.T) {
		me := graph.NodeIdx(1)
		c := setup(t, ExtremumMax, false)
		idx := c.Node().SuggestIndexes(me)

		require.Len(t, idx, 1)
		require.Equal(t, []int{0}, idx[me])
	})

	t.Run("it resolves", func(t *testing.T) {
		c := setup(t, ExtremumMax, false)
		require.Equal(t, []NodeColumn{{c.NarrowBaseID().AsGlobal(), 0}}, c.Node().Resolve(0))
	})
}

package flownode

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/sqltypes"
)

func TestDistinct(t *testing.T) {
	setup := func(t *testing.T, materialized bool) *MockGraph {
		g := NewMockGraph(t)
		s := g.AddBase("source", []string{"x", "y", "z"}, boostpb.TestSchema(sqltypes.Int64, sqltypes.VarChar, sqltypes.Int64), nil)
		g.SetOp(
			"distinct",
			[]string{"x", "y", "z"},
			NewDistinct(s.AsGlobal(), []int{1, 2}),
			materialized,
		)
		return g
	}

	t.Run("simple distinct", func(t *testing.T) {
		g := setup(t, true)

		r1 := boostpb.TestRow(1, "z", 1)
		r2 := boostpb.TestRow(1, "z", 1)
		r3 := boostpb.TestRow(1, "c", 2)

		a := g.NarrowOneRow(r1, true)
		require.Equal(t, r1.AsRecords(), a)

		a = g.NarrowOneRow(r2, true)
		require.Len(t, a, 0)

		a = g.NarrowOneRow(r3, true)
		require.Equal(t, r3.AsRecords(), a)
	})

	t.Run("distinct with negative record", func(t *testing.T) {
		g := setup(t, true)

		r1 := boostpb.TestRow(1, "z", 1)
		r2 := boostpb.TestRow(2, "a", 2)
		r3 := boostpb.TestRow(3, "c", 2)

		a := g.NarrowOneRow(r1, true)
		require.Equal(t, r1.AsRecords(), a)

		a = g.NarrowOneRow(r2, true)
		require.Equal(t, r2.AsRecords(), a)

		a = g.NarrowOneRow(r3, true)
		require.Equal(t, r3.AsRecords(), a)

		g.NarrowOneRow(r1.ToRecord(false), true)
		a = g.NarrowOneRow(r1.ToRecord(true), true)
		require.Equal(t, r1.AsRecords(), a)
	})

	t.Run("multiple records distinct", func(t *testing.T) {
		g := setup(t, true)

		r1 := boostpb.TestRow(1, "z", 1)
		r2 := boostpb.TestRow(2, "a", 2)
		r3 := boostpb.TestRow(3, "c", 2)

		a := g.NarrowOne([]boostpb.Record{
			r2.ToRecord(true),
			r1.ToRecord(true),
			r1.ToRecord(true),
			r3.ToRecord(true),
		}, true)
		require.ElementsMatch(t, []boostpb.Record{r1.AsRecord(), r2.AsRecord(), r3.AsRecord()}, a)

		a = g.NarrowOne([]boostpb.Record{
			r1.ToRecord(false),
			r3.ToRecord(true),
		}, true)
		require.Contains(t, a, r1.ToRecord(false))
		require.NotContains(t, a, r3.ToRecord(true))
	})
}

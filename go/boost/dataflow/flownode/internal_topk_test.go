package flownode

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/server/controller/boostplan/viewplan"
	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/sqltypes"
)

func TestTopk(t *testing.T) {
	setup := func(t *testing.T, desc bool) (*MockGraph, dataflow.IndexPair) {
		cmprows := []viewplan.OrderedColumn{{Col: 2, Desc: desc}}
		g := NewMockGraph(t)
		s := g.AddBase(
			"source",
			[]string{"x", "y", "z"},
			sql.TestSchema(sqltypes.Int64, sqltypes.VarChar, sqltypes.Int64),
		)
		g.SetOp(
			"topk",
			[]string{"x", "y", "z"},
			NewTopK(s.AsGlobal(), cmprows, []int{1}, 3),
			true,
		)
		return g, s
	}

	r12a := sql.TestRow(1, "z", 12)
	r10a := sql.TestRow(2, "z", 10)
	r11a := sql.TestRow(3, "z", 11)
	r05a := sql.TestRow(4, "z", 5)
	r15a := sql.TestRow(5, "z", 15)
	r10b := sql.TestRow(6, "z", 10)
	r10c := sql.TestRow(7, "z", 10)

	t.Run("it keeps topk", func(t *testing.T) {
		g, _ := setup(t, false)
		ni := g.Node().LocalAddr()

		g.NarrowOneRow(r12a, true)
		g.NarrowOneRow(r11a, true)
		g.NarrowOneRow(r05a, true)
		g.NarrowOneRow(r10b, true)
		g.NarrowOneRow(r10c, true)
		require.Equal(t, 3, g.states.Get(ni).Rows())

		g.NarrowOneRow(r15a, true)
		g.NarrowOneRow(r10a, true)
		require.Equal(t, 3, g.states.Get(ni).Rows())
	})

	t.Run("it forwards", func(t *testing.T) {
		g, _ := setup(t, false)

		a := g.NarrowOneRow(r12a, true)
		require.Equal(t, r12a.AsRecords(), a)

		a = g.NarrowOneRow(r10a, true)
		require.Equal(t, r10a.AsRecords(), a)

		a = g.NarrowOneRow(r11a, true)
		require.Equal(t, r11a.AsRecords(), a)

		a = g.NarrowOneRow(r05a, true)
		require.ElementsMatch(t, []sql.Record{r12a.ToRecord(false), r05a.ToRecord(true)}, a)

		a = g.NarrowOneRow(r15a, true)
		require.Len(t, a, 0)
	})

	t.Run("it must query", func(t *testing.T) {
		t.Skip("TODO: upquery when the TopK is not full")

		g, s := setup(t, false)

		g.NarrowOneRow(r12a, true)
		g.NarrowOneRow(r10a, true)
		g.NarrowOneRow(r11a, true)
		g.NarrowOneRow(r05a, true)
		g.NarrowOneRow(r15a, true)

		g.Seed(s, r12a)
		g.Seed(s, r10a)
		g.Seed(s, r11a)
		g.Seed(s, r05a)

		a := g.NarrowOneRow(r15a.ToRecord(false), true)
		require.ElementsMatch(t, []sql.Record{r15a.ToRecord(false), r10a.ToRecord(true)}, a)

		g.Unseed(s)

		a = g.NarrowOneRow(r10b, true)
		require.Len(t, a, 0)

		a = g.NarrowOneRow(r10c, true)
		require.Len(t, a, 0)

		g.Seed(s, r12a)
		g.Seed(s, r11a)
		g.Seed(s, r05a)
		g.Seed(s, r10b)
		g.Seed(s, r10c)

		a = g.NarrowOneRow(r10a.ToRecord(false), true)
		require.Len(t, a, 2)
		require.Equal(t, r10a.ToRecord(false), a[0])
	})

	t.Run("it forwards reversed", func(t *testing.T) {
		g, _ := setup(t, true)

		r12 := sql.TestRow(1, "z", -12.123)
		r10 := sql.TestRow(2, "z", 0.0431)
		r11 := sql.TestRow(3, "z", -0.082)
		r5 := sql.TestRow(4, "z", 5.601)
		r15 := sql.TestRow(5, "z", -15.9)

		a := g.NarrowOneRow(r12, true)
		require.Equal(t, r12.AsRecords(), a)

		a = g.NarrowOneRow(r10, true)
		require.Equal(t, r10.AsRecords(), a)

		a = g.NarrowOneRow(r11, true)
		require.Equal(t, r11.AsRecords(), a)

		a = g.NarrowOneRow(r5, true)
		require.ElementsMatch(t, []sql.Record{r12.ToRecord(false), r5.ToRecord(true)}, a)

		a = g.NarrowOneRow(r15, true)
		require.Len(t, a, 0)
	})

	t.Run("it reports parent columns", func(t *testing.T) {
		g, _ := setup(t, false)

		require.Equal(t,
			[]NodeColumn{{Node: g.NarrowBaseID().AsGlobal(), Column: 0}},
			g.Node().Resolve(0),
		)
		require.Equal(t,
			[]NodeColumn{{Node: g.NarrowBaseID().AsGlobal(), Column: 1}},
			g.Node().Resolve(1),
		)
		require.Equal(t,
			[]NodeColumn{{Node: g.NarrowBaseID().AsGlobal(), Column: 2}},
			g.Node().Resolve(2),
		)
	})

	t.Run("it handles updates", func(t *testing.T) {
		g, _ := setup(t, false)
		ni := g.Node().LocalAddr()

		r1 := sql.TestRow(1, "z", 10)
		r2 := sql.TestRow(2, "z", 10)
		r3 := sql.TestRow(3, "z", 10)
		r4 := sql.TestRow(4, "z", 5)
		r4a := sql.TestRow(4, "z", 10)

		g.NarrowOneRow(r1, true)
		g.NarrowOneRow(r2, true)
		g.NarrowOneRow(r3, true)

		// a positive for a row not in the Top-K should not change the Top-K and shouldn't emit
		// anything
		emit := g.NarrowOneRow(r4, true)
		require.Equal(t, 3, g.states.Get(ni).Rows())
		require.ElementsMatch(t, []sql.Record{r1.ToRecord(false), r4.ToRecord(true)}, emit)

		// should now have 3 rows in Top-K
		// [2, z, 10]
		// [3, z, 10]
		// [4, z, 5]

		emit = g.NarrowOne([]sql.Record{r4.ToRecord(false), r4a.ToRecord(true)}, true)

		// [4, z, 5] is now replaced by [4, z, 10] in the Top-K
		require.ElementsMatch(t, []sql.Record{r4.ToRecord(false), r4a.ToRecord(true)}, emit)

		emit = g.NarrowOne([]sql.Record{r4a.ToRecord(false), r4.ToRecord(true)}, true)

		// now [4, z, 5] is in, BUT we still only keep 3 elements
		// and have to remove one of the existing ones
		require.Equal(t, 3, g.states.Get(ni).Rows())

		if emit[0].Positive {
			require.Equal(t, sqltypes.NewInt64(5), emit[0].Row.ValueAt(2).ToVitessUnsafe())
			require.Equal(t, false, emit[1].Positive)
			require.Equal(t, sqltypes.NewInt64(10), emit[1].Row.ValueAt(2).ToVitessUnsafe())
		} else {
			require.Equal(t, sqltypes.NewInt64(10), emit[0].Row.ValueAt(2).ToVitessUnsafe())
			require.Equal(t, true, emit[1].Positive)
			require.Equal(t, sqltypes.NewInt64(5), emit[1].Row.ValueAt(2).ToVitessUnsafe())
		}
	})
}

func TestTopkNoLimit(t *testing.T) {
	setup := func(t *testing.T, desc bool) (*MockGraph, dataflow.IndexPair) {
		cmprows := []viewplan.OrderedColumn{{Col: 2, Desc: desc}}
		g := NewMockGraph(t)
		s := g.AddBase(
			"source",
			[]string{"x", "y", "z"},
			sql.TestSchema(sqltypes.Int64, sqltypes.VarChar, sqltypes.Int64),
		)
		g.SetOp(
			"topk",
			[]string{"x", "y", "z"},
			NewTopK(s.AsGlobal(), cmprows, []int{1}, -1),
			true,
		)
		return g, s
	}

	r12a := sql.TestRow(1, "z", 12)
	r10a := sql.TestRow(2, "z", 10)
	r11a := sql.TestRow(3, "z", 11)
	r05a := sql.TestRow(4, "z", 5)
	r15a := sql.TestRow(5, "z", 15)
	r10b := sql.TestRow(6, "z", 10)
	r10c := sql.TestRow(7, "z", 10)

	t.Run("it keeps all elements", func(t *testing.T) {
		g, _ := setup(t, false)
		ni := g.Node().LocalAddr()

		g.NarrowOneRow(r12a, true)
		g.NarrowOneRow(r11a, true)
		g.NarrowOneRow(r05a, true)
		g.NarrowOneRow(r10b, true)
		g.NarrowOneRow(r10c, true)
		require.Equal(t, 5, g.states.Get(ni).Rows())

		g.NarrowOneRow(r15a, true)
		g.NarrowOneRow(r10a, true)
		require.Equal(t, 7, g.states.Get(ni).Rows())
	})

	t.Run("it forwards", func(t *testing.T) {
		g, _ := setup(t, false)

		a := g.NarrowOneRow(r12a, true)
		require.Equal(t, r12a.AsRecords(), a)

		a = g.NarrowOneRow(r10a, true)
		require.Equal(t, r10a.AsRecords(), a)

		a = g.NarrowOneRow(r11a, true)
		require.Equal(t, r11a.AsRecords(), a)

		a = g.NarrowOneRow(r05a, true)
		require.ElementsMatch(t, []sql.Record{r05a.ToRecord(true)}, a)

		a = g.NarrowOneRow(r15a, true)
		require.ElementsMatch(t, []sql.Record{r15a.ToRecord(true)}, a)
	})

	t.Run("it forwards reversed", func(t *testing.T) {
		g, _ := setup(t, true)

		r12 := sql.TestRow(1, "z", -12.123)
		r10 := sql.TestRow(2, "z", 0.0431)
		r11 := sql.TestRow(3, "z", -0.082)
		r5 := sql.TestRow(4, "z", 5.601)
		r15 := sql.TestRow(5, "z", -15.9)

		a := g.NarrowOneRow(r12, true)
		require.Equal(t, r12.AsRecords(), a)

		a = g.NarrowOneRow(r10, true)
		require.Equal(t, r10.AsRecords(), a)

		a = g.NarrowOneRow(r11, true)
		require.Equal(t, r11.AsRecords(), a)

		a = g.NarrowOneRow(r5, true)
		require.ElementsMatch(t, []sql.Record{r5.ToRecord(true)}, a)

		a = g.NarrowOneRow(r15, true)
		require.ElementsMatch(t, []sql.Record{r15.ToRecord(true)}, a)
	})

	t.Run("it reports parent columns", func(t *testing.T) {
		g, _ := setup(t, false)

		require.Equal(t,
			[]NodeColumn{{Node: g.NarrowBaseID().AsGlobal(), Column: 0}},
			g.Node().Resolve(0),
		)
		require.Equal(t,
			[]NodeColumn{{Node: g.NarrowBaseID().AsGlobal(), Column: 1}},
			g.Node().Resolve(1),
		)
		require.Equal(t,
			[]NodeColumn{{Node: g.NarrowBaseID().AsGlobal(), Column: 2}},
			g.Node().Resolve(2),
		)
	})

	t.Run("it handles updates", func(t *testing.T) {
		g, _ := setup(t, false)
		ni := g.Node().LocalAddr()

		r1 := sql.TestRow(1, "z", 10)
		r2 := sql.TestRow(2, "z", 10)
		r3 := sql.TestRow(3, "z", 10)
		r4 := sql.TestRow(4, "z", 5)
		r4a := sql.TestRow(4, "z", 10)

		g.NarrowOneRow(r1, true)
		g.NarrowOneRow(r2, true)
		g.NarrowOneRow(r3, true)

		emit := g.NarrowOneRow(r4, true)
		require.Equal(t, 4, g.states.Get(ni).Rows())
		require.ElementsMatch(t, []sql.Record{r4.ToRecord(true)}, emit)

		emit = g.NarrowOne([]sql.Record{r4.ToRecord(false), r4a.ToRecord(true)}, true)

		require.ElementsMatch(t, []sql.Record{r4.ToRecord(false), r4a.ToRecord(true)}, emit)

		emit = g.NarrowOne([]sql.Record{r4a.ToRecord(false), r4.ToRecord(true)}, true)

		require.Equal(t, 4, g.states.Get(ni).Rows())

		if emit[0].Positive {
			require.Equal(t, sqltypes.NewInt64(5), emit[0].Row.ValueAt(2).ToVitessUnsafe())
			require.Equal(t, false, emit[1].Positive)
			require.Equal(t, sqltypes.NewInt64(10), emit[1].Row.ValueAt(2).ToVitessUnsafe())
		} else {
			require.Equal(t, sqltypes.NewInt64(10), emit[0].Row.ValueAt(2).ToVitessUnsafe())
			require.Equal(t, true, emit[1].Positive)
			require.Equal(t, sqltypes.NewInt64(5), emit[1].Row.ValueAt(2).ToVitessUnsafe())
		}
	})
}
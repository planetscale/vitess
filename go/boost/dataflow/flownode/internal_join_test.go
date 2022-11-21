package flownode

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/sqltypes"
)

func TestJoin(t *testing.T) {
	setup := func(t *testing.T) (*MockGraph, boostpb.IndexPair, boostpb.IndexPair) {
		g := NewMockGraph(t)
		l := g.AddBase("left", []string{"l0", "l1"}, boostpb.TestSchema(sqltypes.Int64, sqltypes.VarChar), nil)
		r := g.AddBase("right", []string{"r0", "r1"}, boostpb.TestSchema(sqltypes.Int64, sqltypes.VarChar), nil)
		j := NewJoin(l.AsGlobal(), r.AsGlobal(), JoinTypeOuter, [2]int{0, 0}, []JoinSource{
			JoinSourceBoth(0, 0), JoinSourceLeft(1), JoinSourceRight(1)})
		g.SetOp("join", []string{"j0", "j1", "j2"}, j, false)
		return g, l, r
	}

	t.Run("it describes", func(t *testing.T) {
		j, l, r := setup(t)
		assert.Equal(t,
			fmt.Sprintf("[%v:0, %v:1, %v:1] %v:0 ⋉ %v:0", l, l, r, l, r),
			j.Node().impl.(Internal).Description(true),
		)
	})

	t.Run("it works", func(t *testing.T) {
		j, l, r := setup(t)

		lA1 := boostpb.TestRow(1, "a")
		lB2 := boostpb.TestRow(2, "b")
		lC3 := boostpb.TestRow(3, "c")

		rX1 := boostpb.TestRow(1, "x")
		rY1 := boostpb.TestRow(1, "y")
		rZ2 := boostpb.TestRow(2, "z")
		rW3 := boostpb.TestRow(3, "w")
		rV4 := boostpb.TestRow(4, "")

		rNop := []boostpb.Record{
			boostpb.TestRow(3, "w").ToRecord(false),
			boostpb.TestRow(3, "w").ToRecord(true),
		}

		j.Seed(r, rX1)
		j.Seed(r, rY1)
		j.Seed(r, rZ2)

		j.OneRow(r, rX1, false)
		j.OneRow(r, rY1, false)
		j.OneRow(r, rZ2, false)

		// forward c3 from left; should produce [c3 + None] since no records in right are 3
		nullr := []boostpb.Record{
			boostpb.TestRow(3, "c", nil).ToRecord(true),
		}
		j.Seed(l, lC3)

		rs := j.OneRow(l, lC3, false)
		assert.Equal(t, nullr, rs)

		// doing it again should produce the same result
		j.Seed(l, lC3)
		rs = j.OneRow(l, lC3, false)
		assert.Equal(t, nullr, rs)

		// record from the right should revoke the nulls and replace them with full rows
		j.Seed(r, rW3)
		rs = j.OneRow(r, rW3, false)

		assert.Equal(t, []boostpb.Record{
			boostpb.TestRow(3, "c", nil).ToRecord(false),
			boostpb.TestRow(3, "c", "w").ToRecord(true),
			boostpb.TestRow(3, "c", nil).ToRecord(false),
			boostpb.TestRow(3, "c", "w").ToRecord(true),
		}, rs)

		// Negative followed by positive should not trigger nulls.
		// TODO: it shouldn't trigger any updates at all...
		rs = j.One(r, rNop, false)

		assert.Equal(t, []boostpb.Record{
			boostpb.TestRow(3, "c", "w").ToRecord(false),
			boostpb.TestRow(3, "c", "w").ToRecord(false),
			boostpb.TestRow(3, "c", "w").ToRecord(true),
			boostpb.TestRow(3, "c", "w").ToRecord(true),
		}, rs)

		// forward from left with single matching record on right
		j.Seed(l, lB2)
		rs = j.OneRow(l, lB2, false)

		assert.Equal(t, []boostpb.Record{
			boostpb.TestRow(2, "b", "z").ToRecord(true),
		}, rs)

		// forward from left with two matching records on right
		j.Seed(l, lA1)
		rs = j.OneRow(l, lA1, false)

		assert.Len(t, rs, 2)
		assert.ElementsMatch(t, []boostpb.Record{
			boostpb.TestRow(1, "a", "x").ToRecord(true),
			boostpb.TestRow(1, "a", "y").ToRecord(true),
		}, rs)

		// forward from right with two matching records on left (and one more on right)
		j.Seed(r, rW3)
		rs = j.OneRow(r, rW3, false)

		assert.Equal(t, []boostpb.Record{
			boostpb.TestRow(3, "c", "w").ToRecord(true),
			boostpb.TestRow(3, "c", "w").ToRecord(true),
		}, rs)

		// unmatched forward from right should have no effect
		j.Seed(r, rV4)
		rs = j.OneRow(r, rV4, false)

		assert.Len(t, rs, 0)
	})

	t.Run("it suggests indices", func(t *testing.T) {
		me := graph.NodeIdx(2)
		g, l, r := setup(t)

		hm := map[graph.NodeIdx][]int{
			l.AsGlobal(): {0},
			r.AsGlobal(): {0},
		}

		assert.Equal(t, hm, g.Node().SuggestIndexes(me))
	})

	t.Run("it resolves", func(t *testing.T) {
		g, l, r := setup(t)

		assert.Equal(t, []NodeColumn{{l.AsGlobal(), 0}}, g.Node().Resolve(0))
		assert.Equal(t, []NodeColumn{{l.AsGlobal(), 1}}, g.Node().Resolve(1))
		assert.Equal(t, []NodeColumn{{r.AsGlobal(), 1}}, g.Node().Resolve(2))
	})
}

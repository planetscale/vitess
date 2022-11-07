package flownode

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/sqltypes"
)

func TestUnion(t *testing.T) {
	setup := func(t *testing.T) (*MockGraph, boostpb.IndexPair, boostpb.IndexPair) {
		g := NewMockGraph(t)
		l := g.AddBase("left", []string{"l0", "l1"}, boostpb.TestSchema(sqltypes.Int64, sqltypes.VarChar), nil)
		r := g.AddBase("right", []string{"r0", "r1", "r2"}, boostpb.TestSchema(sqltypes.Int64, sqltypes.VarChar, sqltypes.VarChar), nil)
		emits := map[graph.NodeIdx][]int{
			l.AsGlobal(): {0, 1},
			r.AsGlobal(): {0, 2},
		}
		g.SetOp("union", []string{"u0", "u1"}, NewUnion(emits), false)
		return g, l, r
	}

	t.Run("it describes", func(t *testing.T) {
		u, l, r := setup(t)
		assert.Equal(t,
			fmt.Sprintf("%v:[0 1] ⋃ %v:[0 2]", l, r),
			u.Node().impl.(Internal).Description(true),
		)
	})

	t.Run("it works", func(t *testing.T) {
		u, l, r := setup(t)

		left := boostpb.TestRow(1, "a")
		rsl := u.One(l, left.AsRecords(), false)
		assert.Equal(t, left.AsRecords(), rsl)

		right := boostpb.TestRow(1, "skipped", "x")
		rsr := u.One(r, right.AsRecords(), false)
		assert.Equal(t, boostpb.TestRow(1, "x").AsRecords(), rsr)
	})

	t.Run("it suggests indices", func(t *testing.T) {
		u, _, _ := setup(t)
		me := graph.NodeIdx(1)
		assert.Equal(t, map[graph.NodeIdx][]int(nil), u.Node().SuggestIndexes(me))
	})

	t.Run("it resolves", func(t *testing.T) {
		u, l, r := setup(t)

		r0 := u.Node().Resolve(0)
		assert.ElementsMatch(t, []NodeColumn{
			{l.AsGlobal(), 0},
			{r.AsGlobal(), 0},
		}, r0)

		r1 := u.Node().Resolve(1)
		assert.ElementsMatch(t, []NodeColumn{
			{l.AsGlobal(), 1},
			{r.AsGlobal(), 2},
		}, r1)
	})
}

package flownode

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/sqltypes"
)

func TestIdentity(t *testing.T) {
	setup := func(t *testing.T, materialized bool) *MockGraph {
		var fields = []string{"x", "y", "z"}
		g := NewMockGraph(t)
		s := g.AddBase("source", fields, boostpb.TestSchema(sqltypes.Int64, sqltypes.VarChar, sqltypes.Int64), nil)
		g.SetOp("identity", fields, NewIdentity(s.AsGlobal()), materialized)
		return g
	}

	t.Run("it forwards", func(t *testing.T) {
		g := setup(t, false)
		left := boostpb.TestRow(1, "a")

		row := g.NarrowOneRow(left, false)
		assert.Equal(t, left.AsRecords(), row)
	})

	t.Run("it resolves", func(t *testing.T) {
		g := setup(t, false)

		assert.Equal(t, g.Node().Resolve(0), []NodeColumn{{
			g.NarrowBaseID().AsGlobal(), 0,
		}})

		assert.Equal(t, g.Node().Resolve(1), []NodeColumn{{
			g.NarrowBaseID().AsGlobal(), 1,
		}})

		assert.Equal(t, g.Node().Resolve(2), []NodeColumn{{
			g.NarrowBaseID().AsGlobal(), 2,
		}})
	})
}

package flownode

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/sqltypes"
)

func TestIdentity(t *testing.T) {
	setup := func(t *testing.T, materialized bool) *MockGraph {
		var fields = []string{"x", "y", "z"}
		g := NewMockGraph(t)
		s := g.AddBase("source", fields, sql.TestSchema(sqltypes.Int64, sqltypes.VarChar, sqltypes.Int64))
		g.SetOp("identity", fields, NewIdentity(s.AsGlobal()), materialized)
		return g
	}

	t.Run("it forwards", func(t *testing.T) {
		g := setup(t, false)
		left := sql.TestRow(1, "a")

		row := g.NarrowOneRow(left, false)
		assert.Equal(t, left.AsRecords(), row)
	})

	t.Run("it resolves", func(t *testing.T) {
		g := setup(t, false)

		assert.Equal(t, g.Node().Resolve(0), []NodeColumn{{
			Node: g.NarrowBaseID().AsGlobal(),
		}})

		assert.Equal(t, g.Node().Resolve(1), []NodeColumn{{
			Node: g.NarrowBaseID().AsGlobal(), Column: 1,
		}})

		assert.Equal(t, g.Node().Resolve(2), []NodeColumn{{
			Node: g.NarrowBaseID().AsGlobal(), Column: 2,
		}})
	})
}
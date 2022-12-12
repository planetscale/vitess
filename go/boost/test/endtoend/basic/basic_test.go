package basic

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/server/controller"
	"vitess.io/vitess/go/boost/test/helpers/booste2e"
	"vitess.io/vitess/go/boost/test/helpers/boosttest"
	"vitess.io/vitess/go/sqltypes"
)

func TestEndtoendBasicWithExternalBase(t *testing.T) {
	tt := booste2e.Setup(t, booste2e.WithRecipe("basic"))

	err := tt.BoostTestCluster.Controller().Migrate(context.Background(), func(mig controller.Migration) error {
		schema := boostpb.TestSchema(sqltypes.Int64, sqltypes.Int64)
		a := mig.AddBase("a", []string{"c1", "c2"}, flownode.NewExternalBase([]int{0}, schema, tt.Keyspace, "a"))
		b := mig.AddBase("b", []string{"c1", "c2"}, flownode.NewExternalBase([]int{0}, schema, tt.Keyspace, "b"))

		emits := map[graph.NodeIdx][]int{
			a: {0, 1},
			b: {0, 1},
		}
		u := flownode.NewUnion(emits)
		c := mig.AddIngredient("c", []string{"c1", "c2"}, u, nil)
		mig.Maintain("", "c", c, []int{0}, nil, 0)
		return nil
	})
	require.NoError(t, err, "migration failed")

	var id = sqltypes.NewInt64(1)
	cq := tt.BoostTestCluster.View("c")

	tt.ExecuteFetch("INSERT INTO a VALUES (1, 2)")
	boosttest.Settle()

	cq.Lookup(id).Expect([]sqltypes.Row{{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}})

	tt.ExecuteFetch("INSERT INTO b VALUES (1, 4)")
	boosttest.Settle()

	cq.Lookup(id).Expect([]sqltypes.Row{
		{id, sqltypes.NewInt64(2)},
		{id, sqltypes.NewInt64(4)},
	})

	tt.ExecuteFetch("DELETE FROM a WHERE c1 = 1")
	boosttest.Settle()

	cq.Lookup(id).Expect([]sqltypes.Row{{id, sqltypes.NewInt64(4)}})
	tt.ExecuteFetch("UPDATE b SET c2 = 3 WHERE c1 = 1")
	boosttest.Settle()

	cq.Lookup(id).Expect([]sqltypes.Row{{id, sqltypes.NewInt64(3)}})
}

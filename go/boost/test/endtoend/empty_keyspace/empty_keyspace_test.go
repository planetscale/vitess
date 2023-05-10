package empty_keyspace

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/boost/server/worker"

	"vitess.io/vitess/go/boost/test/helpers/boosttest/testrecipe"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/vtboost"

	"vitess.io/vitess/go/boost/test/helpers/booste2e"
)

func TestEmptyKeyspace(t *testing.T) {
	const DDL = `
	CREATE TABLE num (
		pk BIGINT NOT NULL AUTO_INCREMENT,
		b INT,
		a INT,
		PRIMARY KEY(pk));
`
	const Query = `SELECT b, count(num.a), count(*) FROM num GROUP BY b`

	tt := booste2e.Setup(t, booste2e.WithDDL(DDL))

	for i := 1; i <= 3; i++ {
		tt.ExecuteFetch("INSERT INTO num (a, b) VALUES (%d, 100 * %d)", i, i)
	}
	tt.ExecuteFetch("INSERT INTO num (a, b) VALUES (null, 100)")

	time.Sleep(1 * time.Second)

	recipe := testrecipe.NewRecipeFromSQL(tt, testrecipe.DefaultKeyspace, "", Query)
	_, err := tt.BoostTopo.PutRecipe(context.Background(), &vtboost.PutRecipeRequest{Recipe: recipe.ToProto()})
	require.NoError(t, err)

	tt.ExecuteFetch("SET @@boost_cached_queries = true")

	time.Sleep(1 * time.Second)

	rs := tt.ExecuteFetch(Query)
	require.Len(t, rs.Rows, 3)
	require.ElementsMatch(t, []sqltypes.Row{
		{sqltypes.NewInt32(100), sqltypes.NewInt64(1), sqltypes.NewInt64(2)},
		{sqltypes.NewInt32(200), sqltypes.NewInt64(1), sqltypes.NewInt64(1)},
		{sqltypes.NewInt32(300), sqltypes.NewInt64(1), sqltypes.NewInt64(1)},
	}, rs.Rows)
	tt.BoostTestCluster.AssertWorkerStats(1, worker.StatViewReads)
}

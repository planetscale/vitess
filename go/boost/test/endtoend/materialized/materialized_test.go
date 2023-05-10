package materialized

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/boost/server/worker"
	"vitess.io/vitess/go/boost/test/helpers/booste2e"
	"vitess.io/vitess/go/sqltypes"
)

func TestEndtoendFullyMaterialized(t *testing.T) {
	const DDL = `
	CREATE TABLE num (
		pk BIGINT NOT NULL AUTO_INCREMENT,
		b INT,
		a INT,
		PRIMARY KEY(pk));
`
	const Query = `SELECT b, count(num.a), count(*) FROM num GROUP BY b`

	tt := booste2e.Setup(t, booste2e.WithDDL(DDL), booste2e.WithCachedQueries(Query))

	for i := 1; i <= 3; i++ {
		tt.ExecuteFetch("INSERT INTO num (a, b) VALUES (%d, 100 * %d)", i, i)
	}
	tt.ExecuteFetch("INSERT INTO num (a, b) VALUES (null, 100)")
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
	tt.BoostTestCluster.AssertWorkerStats(4, worker.StatVStreamRows)
}

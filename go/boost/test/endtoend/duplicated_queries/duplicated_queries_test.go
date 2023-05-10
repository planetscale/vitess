package duplicated_queries

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/boost/server/worker"

	"vitess.io/vitess/go/boost/test/helpers/booste2e"
)

func TestMikesProductionSchema(t *testing.T) {
	const DDL = `
CREATE TABLE mike (
	id int unsigned NOT NULL AUTO_INCREMENT,
	season varchar(255),
	PRIMARY KEY (id)
);
`
	const Query1 = `select id from mike where season = ?`
	const Query2 = `select id from mike where season = 'foo'`

	tt := booste2e.Setup(t, booste2e.WithDDL(DDL), booste2e.WithCachedQueries(Query1, Query2))

	for i := 0; i <= 20; i++ {
		tt.ExecuteFetch("INSERT INTO mike (season) VALUES ('season%d')", i%3)
	}
	tt.ExecuteFetch("SET @@boost_cached_queries = true")

	time.Sleep(1 * time.Second)

	rs := tt.ExecuteFetch(`select id from mike where season = 'season1'`)
	require.Len(t, rs.Rows, 7)
	tt.BoostTestCluster.AssertWorkerStats(1, worker.StatViewReads)

	rs = tt.ExecuteFetch(`select id from mike where season = 'foo'`)
	require.Len(t, rs.Rows, 0)
	tt.BoostTestCluster.AssertWorkerStats(2, worker.StatViewReads)
}

package crash

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/boost/test/helpers/booste2e"
)

func TestDropTableDependency(t *testing.T) {
	const DDL = `
CREATE TABLE t (
	id int unsigned NOT NULL AUTO_INCREMENT,
	season varchar(255),
	PRIMARY KEY (id)
);
`
	const Query = `select id from t where id = ?`
	tt := booste2e.Setup(t, booste2e.WithDDL(DDL), booste2e.WithCachedQueries(Query))

	for i := 0; i <= 20; i++ {
		tt.ExecuteFetch("INSERT INTO t (season) VALUES ('season%d')", i%3)
	}
	tt.ExecuteFetch("SET @@boost_cached_queries = true")
	time.Sleep(1 * time.Second)

	rs := tt.ExecuteFetch(`select season from t where id = 1`)
	require.Len(t, rs.Rows, 1)

	tt.ExecuteFetch("DROP TABLE t")
	time.Sleep(3 * time.Second)

	err := tt.ExecuteFetchError(`select season from t where id = 1`)
	require.Error(t, err)
}

package integration

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/boost/test/helpers/boosttest"
	"vitess.io/vitess/go/boost/test/helpers/boosttest/testrecipe"
)

func TestCrashFullMaterialization(t *testing.T) {
	// This test validates what happens if we crash with some unexpected error doing full
	// materialization. This means we need to report the error that happened, and
	// we never install the requested query.
}

func TestCrashPartialMaterialization(t *testing.T) {
	// This test validates what happens if we crash with some unexpected error doing partial
	// materialization while we have a query blocking waiting for the result.
	// We need to unblock the client as soon as we have triggered the error
	// and return the error.
	// The query that failed partial materialization is still marked as not
	// filled and subsequent same queries will fail in the same way, triggering
	// the same error each time.
}

func TestRemovedColumnDependency(t *testing.T) {
	// When we see a column removed that is a dependency for a query, we need to
	// log an error and remove the query from the cache. Subsequent queries
	// would also then fail since they won't work anymore so the cache is safe
	// to delete if this happens.
	//
	// This normally does not happen since we prevent this case with safe DDL enabled,
	// but internally we might be testing things without enabling safe DDL.

	const Recipe = `
	CREATE TABLE num (
	    pk BIGINT NOT NULL AUTO_INCREMENT,
	    a INT,
		b INT,
	PRIMARY KEY(pk));

	SELECT a FROM num;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))
	g.View("q0").Lookup().Expect(`[]`)
	g.TestExecute("INSERT INTO num (a, b) VALUES (1, 2)")
	g.View("q0").Lookup().Expect(`[[INT32(1)]]`)

	g.TestExecute("ALTER TABLE num DROP COLUMN a")
	g.TestExecute("INSERT INTO num (b) VALUES (3)")

	time.Sleep(500 * time.Millisecond)

	require.Nil(t, g.FindView("q0"))
}

func TestRemovedTableDependency(t *testing.T) {
	// When we see a table removed that is a dependency for a query, we need to
	// log an error and remove the query from the cache. Subsequent queries
	// would also then fail since they won't work anymore so the cache is safe
	// to delete if this happens.
	//
	// This normally does not happen since we prevent this case with safe DDL enabled,
	// but internally we might be testing things without enabling safe DDL.

	const Recipe = `
	CREATE TABLE num (
	    pk BIGINT NOT NULL AUTO_INCREMENT,
	    a INT,
		b INT,
	PRIMARY KEY(pk));

	SELECT a FROM num;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))
	g.View("q0").Lookup().Expect(`[]`)
	g.TestExecute("INSERT INTO num (a, b) VALUES (1, 2)")
	g.View("q0").Lookup().Expect(`[[INT32(1)]]`)

	g.TestExecute("DROP TABLE num")
	time.Sleep(500 * time.Millisecond)
	require.Nil(t, g.FindView("q0"))
}

func TestIncompatibleColumnChangeDependency(t *testing.T) {
	// When we see a column change that is incompatible with what we already
	// have cached, we need to log an error and remove it from the cache.
	// This means the same query would not be cached anymore, which can be
	// surprising for the user. The query could still work with a changed column
	// type but that entirely depends on the shape of the query.
	//
	// This normally does not happen since we prevent this case with safe DDL enabled,
	// but internally we might be testing things without enabling safe DDL. So we accept
	// the potential surprising behavior here.

	const Recipe = `
	CREATE TABLE num (
	    pk BIGINT NOT NULL AUTO_INCREMENT,
	    a INT,
		b INT,
	PRIMARY KEY(pk));

	SELECT a FROM num;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))
	g.View("q0").Lookup().Expect(`[]`)
	g.TestExecute("INSERT INTO num (a, b) VALUES (1, 2)")
	g.View("q0").Lookup().Expect(`[[INT32(1)]]`)

	g.TestExecute("ALTER TABLE num MODIFY COLUMN a varchar(255)")
	g.TestExecute("INSERT INTO num (a, b) VALUES ('3', 4)")

	time.Sleep(500 * time.Millisecond)
	require.Nil(t, g.FindView("q0"))
}

func TestColumnStarAdded(t *testing.T) {
	// When we see a column being added to a `select *` expression, we ignore that.
	// This means the query is still cached, but it doesn't change the result of the query.
	//
	// This normally does not happen since we prevent this case with safe DDL enabled,
	// but internally we might be testing things without enabling safe DDL. So we accept
	// the potential surprising behavior here.

	const Recipe = `
	CREATE TABLE num (
	    pk BIGINT NOT NULL AUTO_INCREMENT,
	    a INT,
		b INT,
	PRIMARY KEY(pk));

	SELECT * FROM num;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))
	g.View("q0").Lookup().Expect(`[]`)
	g.TestExecute("INSERT INTO num (a, b) VALUES (1, 2)")

	g.View("q0").Lookup().Expect(`[[INT64(1) INT32(1) INT32(2)]]`)

	g.TestExecute("ALTER TABLE num ADD COLUMN c INT DEFAULT 5")

	g.TestExecute("INSERT INTO num (a, b, c) VALUES (3, 4, 6)")

	time.Sleep(500 * time.Millisecond)

	g.View("q0").Lookup().Expect(`[[INT64(1) INT32(1) INT32(2)] [INT64(2) INT32(3) INT32(4)]]`)
}

package integration

import (
	"testing"

	"vitess.io/vitess/go/boost/test/helpers/boosttest"
	"vitess.io/vitess/go/boost/test/helpers/boosttest/testrecipe"
	"vitess.io/vitess/go/vt/proto/vtboost"
)

func TestProjections(t *testing.T) {
	const Recipe = `
	CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT,
	a INT, b SMALLINT, c TINYINT, d MEDIUMINT, e BIGINT,
	f FLOAT, g DOUBLE, h DECIMAL,
	PRIMARY KEY(pk));

	SELECT num.a, num.a + num.b, 420 FROM num WHERE num.a = :x;
	SELECT num.a, num.a + num.b, 420 FROM num;
	SELECT col1 + col2, 420 FROM (SELECT a as col1, a+b as col2 FROM num) apa WHERE col1 = :x;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 3; i++ {
		g.TestExecute("INSERT INTO num (a, b, c, d, e, f, g, h) VALUES (%d, %d, %d, %d, %d, %d, %d, %d)", i, i, i, i, i, i, i, i)
	}

	g.View("q0").Lookup(1).Expect(`[[INT32(1) INT64(2) INT64(420)]]`)
	g.View("q0").Lookup("1").Expect(`[[INT32(1) INT64(2) INT64(420)]]`)
	g.View("q1").Lookup().Expect(`[[INT32(1) INT64(2) INT64(420)] [INT32(2) INT64(4) INT64(420)] [INT32(3) INT64(6) INT64(420)]]`)
	g.View("q2").Lookup(1).Expect(`[[INT64(3) INT64(420)]]`)
}

func TestProjectionsWithMigration(t *testing.T) {
	const Recipe = `
	CREATE TABLE num (
	    pk BIGINT NOT NULL AUTO_INCREMENT, 
	    a INT, 
	    b SMALLINT, 
	    PRIMARY KEY(pk));

	SELECT num.a, num.a + num.b, 420 FROM num WHERE num.a = :x;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 3; i++ {
		g.TestExecute("INSERT INTO num (a, b) VALUES (%d, %d)", i, i)
	}

	g.View("q0").Lookup(1).Expect(`[[INT32(1) INT64(2) INT64(420)]]`)

	g.AlterRecipe(recipe, "alter table num add column c bigint")

	recipe.Queries = append(recipe.Queries, &vtboost.CachedQuery{
		PublicId: "q1",
		Sql:      "SELECT num.c, num.a + num.b, 420 FROM num WHERE num.a = :x",
		Keyspace: testrecipe.DefaultKeyspace,
	})

	g.ApplyRecipe(recipe)

	g.TestExecute("INSERT INTO num (a, b, c) VALUES (%d, %d, %d)", 10, 10, 666)
	boosttest.Settle()

	g.View("q0").Lookup(1).Expect(`[[INT32(1) INT64(2) INT64(420)]]`)
	g.View("q1").Lookup(10).Expect(`[[INT64(666) INT64(20) INT64(420)]]`)
}

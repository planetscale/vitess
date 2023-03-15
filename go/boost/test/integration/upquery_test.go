package integration

import (
	"testing"

	"vitess.io/vitess/go/boost/test/helpers/boosttest"
	"vitess.io/vitess/go/boost/test/helpers/boosttest/testrecipe"
)

func TestMidFlowUpqueries(t *testing.T) {
	const Recipe = `
	CREATE TABLE num (
	    pk BIGINT NOT NULL AUTO_INCREMENT, 
		b INT,
	    a INT,
	PRIMARY KEY(pk));

	SELECT count(num.a) FROM num WHERE a = ?;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 12; i++ {
		g.TestExecute("INSERT INTO num (a, b) VALUES (%d, 100 * %d)", i%3, i)
	}
	g.TestExecute("INSERT INTO num (a, b) VALUES (null, 100)")

	g.View("q0").Lookup(2).Expect(`[[INT64(4)]]`)
}

func TestMidFlowUpqueriesFully(t *testing.T) {
	const Recipe = `
	CREATE TABLE num (
	    pk BIGINT NOT NULL AUTO_INCREMENT, 
		b INT,
	    a INT,
	PRIMARY KEY(pk));

	SELECT count(num.a), sum(num.b) FROM num;
`
	seed := func(g *boosttest.Cluster) {
		for i := 1; i <= 12; i++ {
			g.TestExecute("INSERT INTO num (a, b) VALUES (%d, 100 * %d)", i, i)
		}
		g.TestExecute("INSERT INTO num (a, b) VALUES (null, 100)")
	}

	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe), boosttest.WithSeed(seed))
	g.View("q0").Lookup().Expect(`[[INT64(12) DECIMAL(7900)]]`)
}

func TestUpqueryInPlan(t *testing.T) {
	const Recipe = `
	CREATE TABLE conv (pk BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	col_int INT, col_double DOUBLE, col_decimal DECIMAL, col_char VARCHAR(255));

	SELECT conv.pk FROM conv WHERE conv.col_char IN ::x;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 3; i++ {
		g.TestExecute("INSERT INTO conv (col_int, col_double, col_decimal, col_char) VALUES (%d, %f, %f, 'string-%d')", i, float64(i), float64(i), i)
	}

	g.View("q0").LookupBvar([]any{"string-1", "string-2"}).Expect(`[[INT64(1)] [INT64(2)]]`)
}

func TestMultipleUpqueries(t *testing.T) {
	const Recipe = `
	CREATE TABLE tbl (pk BIGINT NOT NULL AUTO_INCREMENT,
	a BIGINT, b BIGINT, c BIGINT,
	PRIMARY KEY(pk));

	SELECT tbl.pk FROM tbl WHERE tbl.a = :a and tbl.b = :b and tbl.c = :c;
	SELECT tbl.pk FROM tbl WHERE tbl.a IN ::a and tbl.b = :b;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 16; i++ {
		g.TestExecute("INSERT INTO tbl (a, b, c) VALUES (%d, %d, %d)", i, i%4, i*10)
	}

	upquery0 := g.View("q0")
	upquery0.Lookup(1, 1, 10).Expect(`[[INT64(1)]]`)

	upquery1 := g.View("q1")
	upquery1.LookupBvar([]any{4, 8, 12}, 0).Expect(`[[INT64(4)] [INT64(8)] [INT64(12)]]`)
}

func TestMultiLevelUpqueriesFullyMaterialized(t *testing.T) {
	const Recipe = `
	CREATE TABLE tbl (pk BIGINT NOT NULL AUTO_INCREMENT,
	a BIGINT, b BIGINT, c BIGINT,
	PRIMARY KEY(pk));

	SELECT COUNT(*) FROM (SELECT MAX(a) FROM tbl GROUP BY b) as derived;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 16; i++ {
		g.TestExecute("INSERT INTO tbl (a, b, c) VALUES (%d, %d, %d)", i, i%4, i*10)
	}

	upquery0 := g.View("q0")
	upquery0.Lookup().Expect(`[[INT64(4)]]`)

	g.TestExecute("DELETE FROM tbl WHERE b = 1")

	upquery0.Lookup().Expect(`[[INT64(3)]]`)
}

func TestMultiLevelUpqueriesPartialMaterialized(t *testing.T) {
	const Recipe = `
	CREATE TABLE tbl (pk BIGINT NOT NULL AUTO_INCREMENT,
	a BIGINT, b BIGINT, c BIGINT,
	PRIMARY KEY(pk));

	SELECT COUNT(*) FROM (SELECT MAX(a) FROM tbl WHERE b = ? GROUP BY b) as derived;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 16; i++ {
		g.TestExecute("INSERT INTO tbl (a, b, c) VALUES (%d, %d, %d)", i, i%4, i*10)
	}

	upquery0 := g.View("q0")
	upquery0.Lookup(1).Expect(`[[INT64(1)]]`)
	upquery0.Lookup(420).Expect(`[[INT64(0)]]`)

	g.TestExecute("DELETE FROM tbl WHERE b = 1")

	upquery0.Lookup(1).Expect(`[[INT64(0)]]`)
	upquery0.Lookup(420).Expect(`[[INT64(0)]]`)
}

func TestFullMaterializationsCanReplay(t *testing.T) {
	const Recipe = `
CREATE TABLE t (
	id int NOT NULL,
	PRIMARY KEY (id)
) ENGINE InnoDB,
  CHARSET utf8mb4,
  COLLATE utf8mb4_unicode_ci;

select id from t;
`
	recipe := testrecipe.LoadSQL(t, Recipe)

	seed := func(g *boosttest.Cluster) {
		for i := 0; i < 3; i++ {
			g.TestExecute("insert into t (id) values (%d)", i)
		}
	}
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe), boosttest.WithSeed(seed))
	boosttest.Settle()
	g.View("q0").Lookup().Expect(`[[INT32(0)] [INT32(1)] [INT32(2)]]`)
}

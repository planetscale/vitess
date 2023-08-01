package integration

import (
	"testing"

	"vitess.io/vitess/go/boost/test/helpers/boosttest"
	"vitess.io/vitess/go/boost/test/helpers/boosttest/testrecipe"
)

func TestDatetimeHashed(t *testing.T) {
	const Recipe = `
	CREATE TABLE num (
	    pk BIGINT NOT NULL AUTO_INCREMENT,
	    a datetime,
	PRIMARY KEY(pk));

	SELECT count(*) FROM num WHERE a = ?;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 16; i++ {
		g.TestExecute("INSERT INTO num (a) VALUES ('2010-01-%02d 11:%02d:00')", i, i)
	}

	g.View("q0").Lookup("2010-01-14 11:14:00").Expect(`[[INT64(1)]]`)
	// We can't query for this because the embedded test MySQL server from go-mysql-server
	// doesn't properly convert this to a datetime so the upquery will return the wrong result.
	// Real MySQL will work though so it's not an actual issue.
	g.View("q0").Lookup(20100114111400).Expect(`[[INT64(1)]]`)
}

func TestDatetimeRange(t *testing.T) {
	const Recipe = `
	CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT, a datetime, PRIMARY KEY(pk));
	SELECT num.a, 420 FROM num WHERE num.a > :x;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 16; i++ {
		g.TestExecute(`INSERT INTO num (a) VALUES ('2010-01-%02d 11:%02d:00')`, i, i)
	}

	g.View("q0").Lookup(20100114).Expect(`[
		[DATETIME("2010-01-14 11:14:00") INT64(420)]
		[DATETIME("2010-01-15 11:15:00") INT64(420)]
		[DATETIME("2010-01-16 11:16:00") INT64(420)]
	]`)

	g.View("q0").Lookup("2010-01-14").Expect(`[
		[DATETIME("2010-01-14 11:14:00") INT64(420)]
		[DATETIME("2010-01-15 11:15:00") INT64(420)]
		[DATETIME("2010-01-16 11:16:00") INT64(420)]
	]`)
}

func TestTimestampHashed(t *testing.T) {
	const Recipe = `
	CREATE TABLE num (
	    pk BIGINT NOT NULL AUTO_INCREMENT,
	    a timestamp,
	PRIMARY KEY(pk));

	SELECT count(*) FROM num WHERE a = ?;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 16; i++ {
		g.TestExecute("INSERT INTO num (a) VALUES ('2010-01-%02d 11:%02d:00')", i, i)
	}

	g.View("q0").Lookup("2010-01-14 11:14:00").Expect(`[[INT64(1)]]`)
	// We can't query for this because the embedded test MySQL server from go-mysql-server
	// doesn't properly convert this to a datetime so the upquery will return the wrong result.
	// Real MySQL will work though so it's not an actual issue.
	g.View("q0").Lookup(20100114111400).Expect(`[[INT64(1)]]`)
}

func TestTimestampRange(t *testing.T) {
	const Recipe = `
	CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT, a timestamp, PRIMARY KEY(pk));
	SELECT num.a, 420 FROM num WHERE num.a > :x;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 16; i++ {
		g.TestExecute(`INSERT INTO num (a) VALUES ('2010-01-%02d 11:%02d:00')`, i, i)
	}

	g.View("q0").Lookup(20100114).Expect(`[
		[TIMESTAMP("2010-01-14 11:14:00") INT64(420)]
		[TIMESTAMP("2010-01-15 11:15:00") INT64(420)]
		[TIMESTAMP("2010-01-16 11:16:00") INT64(420)]
	]`)

	g.View("q0").Lookup("2010-01-14").Expect(`[
		[TIMESTAMP("2010-01-14 11:14:00") INT64(420)]
		[TIMESTAMP("2010-01-15 11:15:00") INT64(420)]
		[TIMESTAMP("2010-01-16 11:16:00") INT64(420)]
	]`)
}

func TestDateHashed(t *testing.T) {
	const Recipe = `
	CREATE TABLE num (
	    pk BIGINT NOT NULL AUTO_INCREMENT,
	    a date,
	PRIMARY KEY(pk));

	SELECT count(*) FROM num WHERE a = ?;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 16; i++ {
		g.TestExecute("INSERT INTO num (a) VALUES ('2010-01-%02d')", i)
	}

	g.View("q0").Lookup("2010-01-14").Expect(`[[INT64(1)]]`)
	// We can't query for this because the embedded test MySQL server from go-mysql-server
	// doesn't properly convert this to a datetime so the upquery will return the wrong result.
	// Real MySQL will work though so it's not an actual issue.
	g.View("q0").Lookup(20100114).Expect(`[[INT64(1)]]`)
}

func TestDateRange(t *testing.T) {
	const Recipe = `
	CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT, a date, PRIMARY KEY(pk));
	SELECT num.a, 420 FROM num WHERE num.a > :x;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 16; i++ {
		g.TestExecute(`INSERT INTO num (a) VALUES ('2010-01-%02d')`, i)
	}

	g.View("q0").Lookup(20100114).Expect(`[
		[DATE("2010-01-15") INT64(420)]
		[DATE("2010-01-16") INT64(420)]
	]`)

	g.View("q0").Lookup("2010-01-14").Expect(`[
		[DATE("2010-01-15") INT64(420)]
		[DATE("2010-01-16") INT64(420)]
	]`)
}

func TestTimeHashed(t *testing.T) {
	const Recipe = `
	CREATE TABLE num (
	    pk BIGINT NOT NULL AUTO_INCREMENT,
	    a time,
	PRIMARY KEY(pk));

	SELECT count(*) FROM num WHERE a = ?;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 90; i <= 110; i++ {
		g.TestExecute(`INSERT INTO num (a) VALUES ('%d:00:00')`, i)
	}

	g.View("q0").Lookup("108:00:00").Expect(`[[INT64(1)]]`)
	// We can't query for this because the embedded test MySQL server from go-mysql-server
	// doesn't properly convert this to a datetime so the upquery will return the wrong result.
	// Real MySQL will work though so it's not an actual issue.
	g.View("q0").Lookup(1080000).Expect(`[[INT64(1)]]`)
}

func TestTimeRange(t *testing.T) {
	const Recipe = `
	CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT, a time, PRIMARY KEY(pk));
	SELECT num.a, 420 FROM num WHERE num.a > :x;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 90; i <= 110; i++ {
		g.TestExecute(`INSERT INTO num (a) VALUES ('%d:00:00')`, i)
	}

	g.View("q0").Lookup(1080000).Expect(`[
		[TIME("109:00:00") INT64(420)]
		[TIME("110:00:00") INT64(420)]
	]`)

	g.View("q0").Lookup("108:00:00").Expect(`[
		[TIME("109:00:00") INT64(420)]
		[TIME("110:00:00") INT64(420)]
	]`)
}

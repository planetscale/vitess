package integration

import (
	"testing"

	"vitess.io/vitess/go/boost/test/helpers/boosttest"
	"vitess.io/vitess/go/boost/test/helpers/boosttest/testrecipe"
)

func TestProjectedRange(t *testing.T) {
	const Recipe = `
	CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT, a INT, b INT, c INT, PRIMARY KEY(pk));
	SELECT num.a, num.a + num.b, 420 FROM num WHERE num.a > :x AND num.a < :y;
	SELECT num.a, num.a + num.b, 420 FROM num WHERE num.a < :x AND num.a > :y;
	SELECT num.a, num.a + num.b, 420 FROM num WHERE num.a >= :x;
	SELECT num.a, num.a + num.b, 420 FROM num WHERE num.a < :x;
	SELECT num.a, num.a + num.b, 420 FROM num WHERE num.a <= :x;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 8; i++ {
		g.TestExecute("INSERT INTO num (a, b, c) VALUES (%d, %d, %d)", i, i*2, 69)
	}

	g.View("q0").Lookup(3, 6).Expect(`[
		[INT32(4) INT64(12) INT64(420)]
		[INT32(5) INT64(15) INT64(420)]
	]`)

	g.View("q1").Lookup(6, 3).Expect(`[
		[INT32(4) INT64(12) INT64(420)]
		[INT32(5) INT64(15) INT64(420)]
	]`)

	g.View("q2").Lookup(3).Expect(`[
		[INT32(3) INT64(9) INT64(420)]
		[INT32(4) INT64(12) INT64(420)]
		[INT32(5) INT64(15) INT64(420)]
		[INT32(6) INT64(18) INT64(420)]
		[INT32(7) INT64(21) INT64(420)]
		[INT32(8) INT64(24) INT64(420)]
	]`)

	g.View("q3").Lookup(3).Expect(`[
		[INT32(1) INT64(3) INT64(420)]
		[INT32(2) INT64(6) INT64(420)]
	]`)

	g.View("q4").Lookup(3).Expect(`[
		[INT32(1) INT64(3) INT64(420)]
		[INT32(2) INT64(6) INT64(420)]
		[INT32(3) INT64(9) INT64(420)]
	]`)
}

func TestAggregatedRange(t *testing.T) {
	t.Skipf("TODO: ranges with aggregation operators")

	const Recipe = `
	CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT, a INT, b INT, c INT, PRIMARY KEY(pk));
	SELECT sum(num.a) FROM num WHERE num.a > :x AND num.a < :y;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 8; i++ {
		g.TestExecute("INSERT INTO num (a, b, c) VALUES (%d, %d, %d)", i, i*2, 69)
	}

	g.View("q0").Lookup(3, 6).Expect(`[[INT64(2)]]`)
}

func TestProjectedRangeWithEquality(t *testing.T) {
	const Recipe = `
	CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT, a INT, b INT, c INT, PRIMARY KEY(pk));
	SELECT num.a, num.b, 420 FROM num WHERE num.a > :x AND num.a < :y AND num.b = :w;
	SELECT num.a, num.b, 420 FROM num WHERE num.a > :x AND num.b = :w;
	SELECT num.a, num.b, 420 FROM num WHERE :x < num.a AND num.b = :w;
	SELECT num.a, num.b, 420 FROM num WHERE num.a >= :x AND num.b = :w;
	SELECT num.a, num.b, 420 FROM num WHERE num.a <= :x AND num.b = :w;
	SELECT num.a, num.b, 420 FROM num WHERE num.a < :x AND num.b = :w;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 16; i++ {
		g.TestExecute("INSERT INTO num (a, b, c) VALUES (%d, %d, %d)", i, i%2, 69)
	}

	g.View("q0").Lookup(4, 10, 0).Expect(`[
		[INT32(6) INT32(0) INT64(420)]
		[INT32(8) INT32(0) INT64(420)]
	]`)

	g.View("q1").Lookup(4, 0).Expect(`[
		[INT32(6) INT32(0) INT64(420)]
		[INT32(8) INT32(0) INT64(420)]
		[INT32(10) INT32(0) INT64(420)]
		[INT32(12) INT32(0) INT64(420)]
		[INT32(14) INT32(0) INT64(420)]
		[INT32(16) INT32(0) INT64(420)]
	]`)

	g.View("q2").Lookup(4, 0).Expect(`[
		[INT32(6) INT32(0) INT64(420)]
		[INT32(8) INT32(0) INT64(420)]
		[INT32(10) INT32(0) INT64(420)]
		[INT32(12) INT32(0) INT64(420)]
		[INT32(14) INT32(0) INT64(420)]
		[INT32(16) INT32(0) INT64(420)]
	]`)

	g.View("q3").Lookup(4, 0).Expect(`[
		[INT32(4) INT32(0) INT64(420)]
		[INT32(6) INT32(0) INT64(420)]
		[INT32(8) INT32(0) INT64(420)]
		[INT32(10) INT32(0) INT64(420)]
		[INT32(12) INT32(0) INT64(420)]
		[INT32(14) INT32(0) INT64(420)]
		[INT32(16) INT32(0) INT64(420)]
	]`)

	g.View("q4").Lookup(4, 0).Expect(`[
		[INT32(2) INT32(0) INT64(420)]
		[INT32(4) INT32(0) INT64(420)]
	]`)

	g.View("q5").Lookup(4, 0).Expect(`[
		[INT32(2) INT32(0) INT64(420)]
	]`)
}

func TestProjectedMultipleRangeWithEquality(t *testing.T) {
	const Recipe = `
	CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT, a INT, b INT, c INT, PRIMARY KEY(pk));
	SELECT num.a, num.b, 420 FROM num WHERE num.a > :x AND num.a < :y AND num.b = :w;
	SELECT num.a, num.b, 420 FROM num WHERE num.a > :x AND num.a <= :y AND num.b = :w;
	SELECT num.a, num.b, 420 FROM num WHERE num.a >= :x AND num.a < :y AND num.b = :w;
	SELECT num.a, num.b, 420 FROM num WHERE num.a >= :x AND num.a <= :y AND num.b = :w;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 16; i++ {
		g.TestExecute("INSERT INTO num (a, b, c) VALUES (%d, %d, %d)", i, i%2, 69)
	}

	g.View("q0").Lookup(4, 10, 0).Expect(`[
		[INT32(6) INT32(0) INT64(420)]
		[INT32(8) INT32(0) INT64(420)]
	]`)

	g.View("q1").Lookup(4, 10, 0).Expect(`[
		[INT32(6) INT32(0) INT64(420)]
		[INT32(8) INT32(0) INT64(420)]
		[INT32(10) INT32(0) INT64(420)]
	]`)

	g.View("q2").Lookup(4, 10, 0).Expect(`[
		[INT32(4) INT32(0) INT64(420)]
		[INT32(6) INT32(0) INT64(420)]
		[INT32(8) INT32(0) INT64(420)]
	]`)

	g.View("q3").Lookup(4, 10, 0).Expect(`[
		[INT32(4) INT32(0) INT64(420)]
		[INT32(6) INT32(0) INT64(420)]
		[INT32(8) INT32(0) INT64(420)]
		[INT32(10) INT32(0) INT64(420)]
	]`)
}

func TestProjectedPostProcessing(t *testing.T) {
	const Recipe = `
	CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT, a INT, b INT, c INT, PRIMARY KEY(pk));
	SELECT num.a, num.b, 420 FROM num WHERE num.a > :x AND num.b != :y;
	SELECT num.a, num.b, 420 FROM num WHERE num.a > :x AND num.a < :y AND num.c >= :z AND num.b = :w;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 16; i++ {
		g.TestExecute("INSERT INTO num (a, b, c) VALUES (%d, %d, %d)", i, i%2, i*2)
	}

	g.View("q0").Lookup(4, 1).Expect(`[
		[INT32(6) INT32(0) INT64(420)]
		[INT32(8) INT32(0) INT64(420)]
		[INT32(10) INT32(0) INT64(420)]
		[INT32(12) INT32(0) INT64(420)]
		[INT32(14) INT32(0) INT64(420)]
		[INT32(16) INT32(0) INT64(420)]
	]`)

	g.View("q1").Lookup(4, 14, 16, 0).Expect(`[
		[INT32(8) INT32(0) INT64(420)]
		[INT32(10) INT32(0) INT64(420)]
		[INT32(12) INT32(0) INT64(420)]
	]`)
}

func TestProjectedFilters(t *testing.T) {
	const Recipe = `
	CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT, a INT, b INT, j JSON, PRIMARY KEY(pk));
	SELECT num.a, num.b, 420 FROM num WHERE num.a > :x AND JSON_EXTRACT(num.j, '$.n') = :y;
	SELECT num.a, num.b, 420 FROM num WHERE num.a > :x AND (num.b + 1) = :y;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 16; i++ {
		g.TestExecute(`INSERT INTO num (a, b, j) VALUES (%d, %d, '{"n": %d}')`, i, i%2, i%2)
	}

	g.View("q0").Lookup(4, 0).Expect(`[
		[INT32(6) INT32(0) INT64(420)]
		[INT32(8) INT32(0) INT64(420)]
		[INT32(10) INT32(0) INT64(420)]
		[INT32(12) INT32(0) INT64(420)]
		[INT32(14) INT32(0) INT64(420)]
		[INT32(16) INT32(0) INT64(420)]
	]`)

	g.View("q1").Lookup(4, 1).Expect(`[
		[INT32(6) INT32(0) INT64(420)]
		[INT32(8) INT32(0) INT64(420)]
		[INT32(10) INT32(0) INT64(420)]
		[INT32(12) INT32(0) INT64(420)]
		[INT32(14) INT32(0) INT64(420)]
		[INT32(16) INT32(0) INT64(420)]
	]`)
}

func TestProjectedIn(t *testing.T) {
	const Recipe = `
	CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT, a INT, b INT, j JSON, PRIMARY KEY(pk));
	SELECT num.a, num.b, 420 FROM num WHERE num.a > :x AND JSON_EXTRACT(num.j, '$.n') IN ::w;
	SELECT num.a, num.b, 420 FROM num WHERE num.a > :x AND num.b NOT IN ::w;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 16; i++ {
		g.TestExecute(`INSERT INTO num (a, b, j) VALUES (%d, %d, '{"n": %d}')`, i, i%2, i%2)
	}

	g.View("q0").LookupBvar(4, []any{0, 23, 42}).Expect(`[
		[INT32(6) INT32(0) INT64(420)]
		[INT32(8) INT32(0) INT64(420)]
		[INT32(10) INT32(0) INT64(420)]
		[INT32(12) INT32(0) INT64(420)]
		[INT32(14) INT32(0) INT64(420)]
		[INT32(16) INT32(0) INT64(420)]
	]`)

	g.View("q1").LookupBvar(4, []any{1, 23, 42}).Expect(`[
		[INT32(6) INT32(0) INT64(420)]
		[INT32(8) INT32(0) INT64(420)]
		[INT32(10) INT32(0) INT64(420)]
		[INT32(12) INT32(0) INT64(420)]
		[INT32(14) INT32(0) INT64(420)]
		[INT32(16) INT32(0) INT64(420)]
	]`)
}

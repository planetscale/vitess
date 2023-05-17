package integration

import (
	"fmt"
	"strings"
	"testing"

	"vitess.io/vitess/go/boost/dataflow/domain"
	"vitess.io/vitess/go/boost/test/helpers/boosttest"
	"vitess.io/vitess/go/boost/test/helpers/boosttest/testrecipe"
	"vitess.io/vitess/go/slices2"
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

	g.AssertWorkerStats(0, domain.StatUpquery)

	g.View("q0").Lookup(4, 10, 0).Expect(`[
		[INT32(6) INT32(0) INT64(420)]
		[INT32(8) INT32(0) INT64(420)]
	]`)

	g.AssertWorkerStats(1, domain.StatUpquery)

	g.View("q1").Lookup(4, 10, 0).Expect(`[
		[INT32(6) INT32(0) INT64(420)]
		[INT32(8) INT32(0) INT64(420)]
		[INT32(10) INT32(0) INT64(420)]
	]`)

	g.AssertWorkerStats(2, domain.StatUpquery)

	g.View("q2").Lookup(4, 10, 0).Expect(`[
		[INT32(4) INT32(0) INT64(420)]
		[INT32(6) INT32(0) INT64(420)]
		[INT32(8) INT32(0) INT64(420)]
	]`)

	g.AssertWorkerStats(3, domain.StatUpquery)

	g.View("q3").Lookup(4, 10, 0).Expect(`[
		[INT32(4) INT32(0) INT64(420)]
		[INT32(6) INT32(0) INT64(420)]
		[INT32(8) INT32(0) INT64(420)]
		[INT32(10) INT32(0) INT64(420)]
	]`)

	g.AssertWorkerStats(4, domain.StatUpquery)
}

type comparison struct {
	sql string
	arg any
}

func perm(a []comparison, f func([]comparison)) {
	perm1(a, f, 0)
}

func perm1(a []comparison, f func([]comparison), i int) {
	if i > len(a) {
		f(a)
		return
	}
	perm1(a, f, i+1)
	for j := i + 1; j < len(a); j++ {
		a[i], a[j] = a[j], a[i]
		perm1(a, f, i+1)
		a[i], a[j] = a[j], a[i]
	}
}

func TestRangeWithEqualityDifferentTypes(t *testing.T) {
	tCases := []struct {
		comparisons []comparison
		expected    string
	}{
		{
			comparisons: []comparison{
				{sql: "num.a > :a1", arg: "text-04"},
				{sql: "num.a < :a2", arg: "text-10"},
				{sql: "num.b = :b", arg: 0},
				{sql: "num.c = :c", arg: 69},
			},
			expected: `[
				[VARCHAR("text-06") INT32(0) INT64(420)]
				[VARCHAR("text-08") INT32(0) INT64(420)]
			]`,
		},
		{
			comparisons: []comparison{
				{sql: "num.a > :a1", arg: "text-04"},
				{sql: "num.a < :a2", arg: "text-10"},
				{sql: "num.b = :b", arg: 0},
			},
			expected: `[
				[VARCHAR("text-06") INT32(0) INT64(420)]
				[VARCHAR("text-08") INT32(0) INT64(420)]
			]`,
		},
		{
			comparisons: []comparison{
				{sql: "num.a > :a1", arg: "text-04"},
				{sql: "num.b = :b", arg: 0},
			},
			expected: `[
				[VARCHAR("text-06") INT32(0) INT64(420)]
				[VARCHAR("text-08") INT32(0) INT64(420)]
				[VARCHAR("text-10") INT32(0) INT64(420)]
				[VARCHAR("text-12") INT32(0) INT64(420)]
				[VARCHAR("text-14") INT32(0) INT64(420)]
				[VARCHAR("text-16") INT32(0) INT64(420)]
			]`,
		},
	}

	for _, tc := range tCases {
		perm(tc.comparisons, func(a []comparison) {
			sql := strings.Join(slices2.Map(a, func(c comparison) string { return c.sql }), " AND ")
			t.Run(sql, func(t *testing.T) {
				var buf strings.Builder
				buf.WriteString("CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT, a varchar(255), b int, c bigint, PRIMARY KEY(pk));\n")
				fmt.Fprintf(&buf, "SELECT num.a, num.b, 420 FROM num WHERE %s;", sql)

				recipe := testrecipe.LoadSQL(t, buf.String())
				g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

				for i := 1; i <= 16; i++ {
					g.TestExecute("INSERT INTO num (a, b, c) VALUES ('text-%02d', %d, %d)", i, i%2, 69)
				}

				g.AssertWorkerStats(0, domain.StatUpquery)
				args := slices2.Map(a, func(c comparison) any { return c.arg })
				g.View("q0").Lookup(args...).Expect(tc.expected)
				g.AssertWorkerStats(1, domain.StatUpquery)
			})
		})
	}
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

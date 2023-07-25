package integration

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"text/template"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/boost/server/controller/config"
	"vitess.io/vitess/go/boost/test/helpers/boosttest"
	"vitess.io/vitess/go/boost/test/helpers/boosttest/testexecutor"
	"vitess.io/vitess/go/boost/test/helpers/boosttest/testrecipe"
)

func TestMidflowAggregation(t *testing.T) {
	const Recipe = `
	CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT, b INT, a INT, PRIMARY KEY(pk));
	SELECT count(num.a), count(*) FROM num;`

	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 3; i++ {
		g.TestExecute("INSERT INTO num (a, b) VALUES (%d, 100 * %d)", i, i)
	}
	g.TestExecute("INSERT INTO num (a, b) VALUES (null, 100)")

	g.View("q0").Lookup().Expect(`[[INT64(3) INT64(4)]]`)
}

func TestAggregations(t *testing.T) {
	const Recipe = `
	CREATE TABLE num (
	    pk BIGINT NOT NULL AUTO_INCREMENT, 
		b INT,
	    a INT,
	PRIMARY KEY(pk));

	SELECT count(num.a), count(*) FROM num;
	SELECT b, count(num.a), count(*) FROM num GROUP BY b;
	SELECT count(num.a), count(*) FROM num WHERE a = ?;
	SELECT count(a) FROM num GROUP BY b;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 3; i++ {
		g.TestExecute("INSERT INTO num (a, b) VALUES (%d, 100 * %d)", i, i)
	}
	g.TestExecute("INSERT INTO num (a, b) VALUES (null, 100)")

	g.View("q0").Lookup().Expect(`[[INT64(3) INT64(4)]]`)
	g.View("q1").Lookup().Expect(`[[INT32(100) INT64(1) INT64(2)] [INT32(200) INT64(1) INT64(1)] [INT32(300) INT64(1) INT64(1)]]`)
	g.View("q2").Lookup(2).Expect(`[[INT64(1) INT64(1)]]`)
	g.View("q2").Lookup(420).Expect(`[[INT64(0) INT64(0)]]`)
}

func TestAggregationsAverage(t *testing.T) {
	const Recipe = `
	CREATE TABLE num (
	    pk BIGINT NOT NULL AUTO_INCREMENT, 
		b INT,
	    a INT,
	PRIMARY KEY(pk));

	SELECT avg(num.a) FROM num;
	SELECT b, avg(num.a) FROM num GROUP BY b;
	SELECT count(num.a), avg(num.a) FROM num;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 3; i++ {
		g.TestExecute("INSERT INTO num (a, b) VALUES (%d, 100 * %d)", i, i)
	}
	g.TestExecute("INSERT INTO num (a, b) VALUES (null, 100)")

	g.View("q0").Lookup().Expect(`[[DECIMAL(2.0000)]]`)
	g.View("q1").Lookup().Expect(`[[INT32(100) DECIMAL(1.0000)] [INT32(200) DECIMAL(2.0000)] [INT32(300) DECIMAL(3.0000)]]`)
	g.View("q2").Lookup().Expect(`[[INT64(3) DECIMAL(2.0000)]]`)
}

func TestAggregationsWithFullExternalReplay(t *testing.T) {
	const Recipe = `
	CREATE TABLE num (
	    pk BIGINT NOT NULL AUTO_INCREMENT,
		b INT,
	    a INT,
	PRIMARY KEY(pk));

	SELECT count(num.a), count(*) FROM num;
	SELECT b, count(num.a), count(*) FROM num GROUP BY b;
	SELECT count(num.a), count(*) FROM num WHERE a = ?;
`
	seed := func(g *boosttest.Cluster) {
		for i := 1; i <= 3; i++ {
			g.TestExecute("INSERT INTO num (a, b) VALUES (%d, 100 * %d)", i, i)
		}
		g.TestExecute("INSERT INTO num (a, b) VALUES (null, 100)")

	}

	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe), boosttest.WithSeed(seed))
	g.View("q0").Lookup().Expect(`[[INT64(3) INT64(4)]]`)
	g.View("q1").Lookup().Expect(`[[INT32(100) INT64(1) INT64(2)] [INT32(200) INT64(1) INT64(1)] [INT32(300) INT64(1) INT64(1)]]`)
	g.View("q2").Lookup(2).Expect(`[[INT64(1) INT64(1)]]`)
}

func TestAggregationPermutations(t *testing.T) {
	const RecipeTemplate = `
	CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT, a {{.Type}}, b {{.Type}}, PRIMARY KEY(pk));
	SELECT MIN(num.a) FROM num{{if .Filter}} WHERE {{.Filter}}{{end}}{{if .Group}} GROUP BY {{.Group}}{{end}};
	SELECT MAX(num.a) FROM num{{if .Filter}} WHERE {{.Filter}}{{end}}{{if .Group}} GROUP BY {{.Group}}{{end}};
	SELECT SUM(num.a) FROM num{{if .Filter}} WHERE {{.Filter}}{{end}}{{if .Group}} GROUP BY {{.Group}}{{end}};
`
	recipeTemplate := template.Must(template.New("recipe").Parse(RecipeTemplate))

	type Recipe struct {
		Type   string
		Filter string
		Group  string

		From          int
		To            int
		Min, Max, Sum string
	}

	queries := []Recipe{
		{Type: "BIGINT", From: 0, To: 17, Min: "[[INT64(0)]]", Max: "[[INT64(16)]]", Sum: "[[DECIMAL(136)]]"},
		{Type: "BIGINT", From: 1, To: 17, Min: "[[INT64(1)]]", Max: "[[INT64(16)]]", Sum: "[[DECIMAL(136)]]"},
		{Type: "BIGINT", Filter: "b = 0", From: 0, To: 17, Min: "[[INT64(0)]]", Max: "[[INT64(16)]]", Sum: "[[DECIMAL(40)]]"},
		{Type: "BIGINT", Filter: "b = 420", From: 0, To: 17, Min: "[[NULL]]", Max: "[[NULL]]", Sum: "[[NULL]]"},
		{Type: "BIGINT", From: 0, To: 0, Min: "[[NULL]]", Max: "[[NULL]]", Sum: "[[NULL]]"},
		{Type: "DOUBLE", From: 0, To: 17, Min: "[[FLOAT64(0)]]", Max: "[[FLOAT64(16)]]", Sum: "[[FLOAT64(136)]]"},
		{Type: "DOUBLE", From: 1, To: 17, Min: "[[FLOAT64(1)]]", Max: "[[FLOAT64(16)]]", Sum: "[[FLOAT64(136)]]"},
		{Type: "DOUBLE", Filter: "b = 0", From: 0, To: 17, Min: "[[FLOAT64(0)]]", Max: "[[FLOAT64(16)]]", Sum: "[[FLOAT64(40)]]"},
		{Type: "DOUBLE", Filter: "b = 420", From: 0, To: 17, Min: "[[NULL]]", Max: "[[NULL]]", Sum: "[[NULL]]"},
		{Type: "DOUBLE", From: 0, To: 0, Min: "[[NULL]]", Max: "[[NULL]]", Sum: "[[NULL]]"},
		{Type: "DECIMAL", From: 0, To: 17, Min: "[[DECIMAL(0)]]", Max: "[[DECIMAL(16)]]", Sum: "[[DECIMAL(136)]]"},
		{Type: "DECIMAL", From: 1, To: 17, Min: "[[DECIMAL(1)]]", Max: "[[DECIMAL(16)]]", Sum: "[[DECIMAL(136)]]"},
		{Type: "DECIMAL", Filter: "b = 0", From: 0, To: 17, Min: "[[DECIMAL(0)]]", Max: "[[DECIMAL(16)]]", Sum: "[[DECIMAL(40)]]"},
		{Type: "DECIMAL", Filter: "b = 420", From: 0, To: 17, Min: "[[NULL]]", Max: "[[NULL]]", Sum: "[[NULL]]"},
		{Type: "DECIMAL", From: 0, To: 0, Min: "[[NULL]]", Max: "[[NULL]]", Sum: "[[NULL]]"},
		{Type: "YEAR", From: 2000, To: 2017, Min: "[[YEAR(2000)]]", Max: "[[YEAR(2016)]]", Sum: "[[DECIMAL(34136)]]"},
		{Type: "YEAR", From: 1, To: 17, Min: "[[YEAR(2001)]]", Max: "[[YEAR(2016)]]", Sum: "[[DECIMAL(32136)]]"},
		{Type: "YEAR", Filter: "b = 0", From: 2000, To: 2017, Min: "[[YEAR(2000)]]", Max: "[[YEAR(2016)]]", Sum: "[[DECIMAL(10040)]]"},
		{Type: "YEAR", Filter: "b = 420", From: 0, To: 17, Min: "[[NULL]]", Max: "[[NULL]]", Sum: "[[NULL]]"},
		{Type: "YEAR", From: 0, To: 0, Min: "[[NULL]]", Max: "[[NULL]]", Sum: "[[NULL]]"},
	}

	for _, qq := range queries {
		t.Run(fmt.Sprintf("%s_%s_%s", qq.Type, qq.Filter, qq.Group), func(t *testing.T) {
			var recipeSQL strings.Builder
			require.NoError(t, recipeTemplate.Execute(&recipeSQL, qq))

			recipe := testrecipe.LoadSQL(t, recipeSQL.String())
			g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

			for i := qq.From; i < qq.To; i++ {
				g.TestExecute("INSERT INTO num (a, b) VALUES (%d, %d)", i, i%4)
			}

			g.View("q0").Lookup().Expect(qq.Min)
			g.View("q1").Lookup().Expect(qq.Max)
			g.View("q2").Lookup().Expect(qq.Sum)
		})
	}
}

func TestExtremumComparableTypes(t *testing.T) {
	const RecipeTemplate = `
	CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT, a {{.Type}}, b bigint, PRIMARY KEY(pk)){{if .Collation}} COLLATE {{.Collation}}{{end}};
	SELECT MIN(num.a) FROM num{{if .Filter}} WHERE {{.Filter}}{{end}}{{if .Group}} GROUP BY {{.Group}}{{end}};
	SELECT MAX(num.a) FROM num{{if .Filter}} WHERE {{.Filter}}{{end}}{{if .Group}} GROUP BY {{.Group}}{{end}};
`
	recipeTemplate := template.Must(template.New("recipe").Parse(RecipeTemplate))

	type Recipe struct {
		Type      string
		Filter    string
		Collation string
		Group     string

		Values   []string
		Min, Max string
	}

	queries := []Recipe{
		{Type: "DATE", Values: []string{"0000-00-00", "2010-01-01", "2010-01-02", "2011-01-01", "2011-10-05"}, Min: "[[DATE(\"0000-00-00\")]]", Max: "[[DATE(\"2011-10-05\")]]"},
		{Type: "DATE", Filter: "b = 0", Values: []string{"2010-01-01", "2010-01-02", "2011-01-01"}, Min: "[[DATE(\"2010-01-01\")]]", Max: "[[DATE(\"2011-01-01\")]]"},
		{Type: "DATE", Filter: "b = 420", Values: []string{"2010-01-01", "2010-01-02", "2011-01-01"}, Min: "[[NULL]]", Max: "[[NULL]]"},
		{Type: "DATE", Values: nil, Min: "[[NULL]]", Max: "[[NULL]]"},
		{Type: "DATETIME", Values: []string{"0000-00-00 00:00:00", "2010-01-01 01:00:00", "2010-01-02 00:30:00", "2011-01-01 15:00:00", "2011-10-05 10:00:00"}, Min: "[[DATETIME(\"0000-00-00 00:00:00\")]]", Max: "[[DATETIME(\"2011-10-05 10:00:00\")]]"},
		{Type: "DATETIME", Filter: "b = 0", Values: []string{"2010-01-01 01:00:00", "2010-01-02 00:30:00", "2011-01-01 15:00:00", "2011-10-05 10:00:00"}, Min: "[[DATETIME(\"2010-01-01 01:00:00\")]]", Max: "[[DATETIME(\"2011-01-01 15:00:00\")]]"},
		{Type: "DATETIME", Filter: "b = 420", Values: []string{"2010-01-01 01:00:00", "2010-01-02 00:30:00", "2011-01-01 15:00:00", "2011-10-05 10:00:00"}, Min: "[[NULL]]", Max: "[[NULL]]"},
		{Type: "DATETIME", Values: nil, Min: "[[NULL]]", Max: "[[NULL]]"},
		{Type: "TIMESTAMP", Values: []string{"0000-00-00 00:00:00", "2010-01-01 01:00:00", "2010-01-02 00:30:00", "2011-01-01 15:00:00", "2011-10-05 10:00:00"}, Min: "[[TIMESTAMP(\"0000-00-00 00:00:00\")]]", Max: "[[TIMESTAMP(\"2011-10-05 10:00:00\")]]"},
		{Type: "TIMESTAMP", Filter: "b = 0", Values: []string{"2010-01-01 01:00:00", "2010-01-02 00:30:00", "2011-01-01 15:00:00", "2011-10-05 10:00:00"}, Min: "[[TIMESTAMP(\"2010-01-01 01:00:00\")]]", Max: "[[TIMESTAMP(\"2011-01-01 15:00:00\")]]"},
		{Type: "TIMESTAMP", Filter: "b = 420", Values: []string{"2010-01-01 01:00:00", "2010-01-02 00:30:00", "2011-01-01 15:00:00", "2011-10-05 10:00:00"}, Min: "[[NULL]]", Max: "[[NULL]]"},
		{Type: "TIMESTAMP", Values: nil, Min: "[[NULL]]", Max: "[[NULL]]"},
		{Type: "varchar(255)", Collation: "utf8mb4_0900_ai_ci", Values: []string{"foo", "bar", "Foo", "Bar"}, Min: `[[VARCHAR("bar")]]`, Max: `[[VARCHAR("foo")]]`},
		{Type: "varchar(255)", Collation: "utf8mb4_0900_bin", Values: []string{"foo", "bar", "Foo", "Bar"}, Min: `[[VARCHAR("Bar")]]`, Max: `[[VARCHAR("foo")]]`},
	}

	for _, qq := range queries {
		t.Run(fmt.Sprintf("%s_%s_%s", qq.Type, qq.Filter, qq.Group), func(t *testing.T) {
			var recipeSQL strings.Builder
			require.NoError(t, recipeTemplate.Execute(&recipeSQL, qq))

			recipe := testrecipe.LoadSQL(t, recipeSQL.String())
			g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

			for i, v := range qq.Values {
				g.TestExecute("INSERT INTO num (a, b) VALUES ('%s', %d)", v, i%2)
			}

			g.View("q0").Lookup().Expect(qq.Min)
			g.View("q1").Lookup().Expect(qq.Max)
		})
	}
}

func TestCustomBaseTypes(t *testing.T) {
	const Recipe = `
CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT,
	a INT, b SMALLINT, c TINYINT, d MEDIUMINT, e BIGINT,
	f FLOAT, g DOUBLE, h DECIMAL,
	PRIMARY KEY(pk));

	SELECT /*vt+ VIEW=identity */ num.a, num.b, num.c, num.d, num.e, num.f, num.g, num.h
	FROM num WHERE num.pk = :primary;

	SELECT /*vt+ VIEW=summed_g */ SUM(num.g) FROM num;
	
	SELECT /*vt+ VIEW=max_a */ MAX(num.a) FROM num;
	SELECT /*vt+ VIEW=max_g */ MAX(num.g) FROM num;
	
	SELECT /*vt+ VIEW=min_a */ MIN(num.a) FROM num;
	SELECT /*vt+ VIEW=min_g */ MIN(num.g) FROM num;
	SELECT /*vt+ VIEW=summed_a */ SUM(num.a) FROM num;
	SELECT /*vt+ VIEW=multi */ SUM(num.a), MIN(num.g) FROM num;
`

	executor := func(options *testexecutor.Options) {
		options.MaxBatchSize = 1
	}

	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe), boosttest.WithFakeExecutorOptions(executor))

	for i := 1; i <= 3; i++ {
		g.TestExecute("INSERT INTO num (a, b, c, d, e, f, g, h) VALUES (%d, %d, %d, %d, %d, %d, %d, %d)", i, i, i, i, i, i, i, i)
	}

	identity := g.View("identity").Lookup(1).ExpectLen(1)
	t.Logf("lookup identity: %v", identity.Rows)

	g.View("summed_a").Lookup().Expect(`[[DECIMAL(6)]]`)
	g.View("summed_g").Lookup().Expect(`[[FLOAT64(6)]]`)

	g.View("max_a").Lookup().Expect(`[[INT32(3)]]`)
	g.View("max_g").Lookup().Expect(`[[FLOAT64(3)]]`)

	g.View("min_a").Lookup().Expect(`[[INT32(1)]]`)
	g.View("min_g").Lookup().Expect(`[[FLOAT64(1)]]`)
	g.View("multi").Lookup().Expect(`[[DECIMAL(6) FLOAT64(1)]]`)
}

func TestExtremumsCrash(t *testing.T) {
	const Recipe = `
       CREATE TABLE num (
           pk BIGINT NOT NULL AUTO_INCREMENT,
               b INT,
           a INT,
       PRIMARY KEY(pk));
       SELECT /*vt+ VIEW=op0 PUBLIC */ max(a) FROM num WHERE b = :b;
`

	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 0; i < 12; i++ {
		g.TestExecute("INSERT INTO num (a, b) VALUES (%d, 100)", i%4)
	}

	op0 := g.View("op0")
	op0.Lookup(100).Expect(`[[INT32(3)]]`)

	g.TestExecute("DELETE FROM num WHERE a = 3")
	op0.Lookup(100).Expect(`[[INT32(2)]]`)
}

func TestExtremumsCrashFullMaterialization(t *testing.T) {
	const Recipe = `
       CREATE TABLE num (
           pk BIGINT NOT NULL AUTO_INCREMENT,
               b INT,
           a INT,
       PRIMARY KEY(pk));
       SELECT max(a) FROM num;
`

	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 0; i < 12; i++ {
		g.TestExecute("INSERT INTO num (a, b) VALUES (%d, 100)", i%4)
	}

	op0 := g.View("q0")
	op0.Lookup().Expect(`[[INT32(3)]]`)

	g.TestExecute("DELETE FROM num WHERE a = 3")
	op0.Lookup().Expect(`[[INT32(2)]]`)
}

func TestAggregation(t *testing.T) {
	queries := `
CREATE TABLE vote (id bigint NOT NULL AUTO_INCREMENT, article_id bigint, user bigint, PRIMARY KEY(id));

SELECT user, sum(CASE WHEN article_id = 5 THEN 1 ELSE 0 END) AS sum FROM vote GROUP BY user
`
	recipe := testrecipe.LoadSQL(t, queries)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	g.TestExecute("INSERT INTO vote(id, article_id, user) VALUES (1, 1, 1), (2, 5, 2), (3, 2, 2), (4, 5, 2)")

	g.View("q0").Lookup().Expect(`[[INT64(2) DECIMAL(2)] [INT64(1) DECIMAL(0)]]`)
}

func TestEmptyPartialMaterializationAggregations(t *testing.T) {
	const Recipe = `
CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT, a BIGINT, b BIGINT, PRIMARY KEY(pk));
SELECT SUM(num.a), COUNT(num.b) FROM num WHERE num.a = :a;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 0; i < 12; i++ {
		g.TestExecute("INSERT INTO num (a, b) VALUES (%d, %d)", i, 10*i)
	}

	agg0 := g.View("q0")
	agg0.Lookup(2).Expect(`[[DECIMAL(2) INT64(1)]]`)
	agg0.Lookup(420).Expect(`[[NULL INT64(0)]]`)

	g.TestExecute("INSERT INTO num (a, b) VALUES (%d, %d)", 420, 666)
	agg0.Lookup(420).Expect(`[[DECIMAL(420) INT64(1)]]`)
}

func TestEmptyFullMaterializationAggregations(t *testing.T) {
	const Recipe = `
CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT, a BIGINT, b BIGINT, PRIMARY KEY(pk));
SELECT SUM(num.a), COUNT(num.b) FROM num;
SELECT SUM(num.a), COUNT(num.b) FROM num GROUP BY num.a;
SELECT SUM(num.a), COUNT(num.b) FROM num WHERE num.pk = :pk;
SELECT SUM(num.a), COUNT(num.b) FROM num WHERE num.pk = :pk GROUP BY num.pk;
`

	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	agg0 := g.View("q0")
	agg1 := g.View("q1")
	agg2 := g.View("q2")
	agg3 := g.View("q3")

	agg0.Lookup().Expect(`[[NULL INT64(0)]]`)
	agg1.Lookup().Expect(`[]`)
	agg2.Lookup(4).Expect(`[[NULL INT64(0)]]`)
	agg3.Lookup(4).Expect(`[]`)

	for i := 0; i < 3; i++ {
		g.TestExecute("INSERT INTO num (a, b) VALUES (%d, %d)", i, 10*i)
	}

	agg0.Lookup().Expect(`[[DECIMAL(3) INT64(3)]]`)
	agg1.Lookup().Expect(`[[DECIMAL(2) INT64(1)] [DECIMAL(1) INT64(1)] [DECIMAL(0) INT64(1)]]`)
	agg2.Lookup(4).Expect(`[[NULL INT64(0)]]`)
	agg3.Lookup(4).Expect(`[]`)

	g.TestExecute("INSERT INTO num (a, b) VALUES (%d, %d)", 420, 420)

	agg0.Lookup().Expect(`[[DECIMAL(423) INT64(4)]]`)
	agg1.Lookup().Expect(`[[DECIMAL(420) INT64(1)] [DECIMAL(2) INT64(1)] [DECIMAL(1) INT64(1)] [DECIMAL(0) INT64(1)]]`)
	agg2.Lookup(4).Expect(`[[DECIMAL(420) INT64(1)]]`)
	agg3.Lookup(4).Expect(`[[DECIMAL(420) INT64(1)]]`)
}

func TestAggregationCountsToZero(t *testing.T) {
	const Recipe = `
       CREATE TABLE num ( pk BIGINT NOT NULL AUTO_INCREMENT, b INT, a INT, PRIMARY KEY(pk));
       SELECT count(a), b FROM num GROUP BY b;
       SELECT count(a) FROM num;
`

	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 4; i++ {
		g.TestExecute("INSERT INTO num (a, b) VALUES (%d, %d)", i, i*10)
	}

	q0 := g.View("q0")
	q0.Lookup().Expect("[[INT64(1) INT32(40)] [INT64(1) INT32(30)] [INT64(1) INT32(20)] [INT64(1) INT32(10)]]")

	q1 := g.View("q1")
	q1.Lookup().Expect("[[INT64(4)]]")

	g.TestExecute("DELETE FROM num WHERE b = 20")

	q0.Lookup().Expect("[[INT64(1) INT32(40)] [INT64(1) INT32(30)] [INT64(1) INT32(10)]]")
	q1.Lookup().Expect("[[INT64(3)]]")

	g.TestExecute("DELETE FROM num")

	q0.Lookup().Expect("[]")
	q1.Lookup().Expect("[[INT64(0)]]")
}

func TestAggregationSumsToZero(t *testing.T) {
	const Recipe = `
       CREATE TABLE num ( pk BIGINT NOT NULL AUTO_INCREMENT, b INT, a INT, PRIMARY KEY(pk));
       SELECT sum(a), b FROM num GROUP BY b;
       SELECT sum(a) FROM num;
`

	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 4; i++ {
		g.TestExecute("INSERT INTO num (a, b) VALUES (%d, %d)", i, i*10)
	}

	q0 := g.View("q0")
	q0.Lookup().Expect("[[DECIMAL(4) INT32(40)] [DECIMAL(3) INT32(30)] [DECIMAL(2) INT32(20)] [DECIMAL(1) INT32(10)]]")

	q1 := g.View("q1")
	q1.Lookup().Expect("[[DECIMAL(10)]]")

	g.TestExecute("DELETE FROM num WHERE b = 20")

	q0.Lookup().Expect("[[DECIMAL(4) INT32(40)] [DECIMAL(3) INT32(30)] [DECIMAL(1) INT32(10)]]")
	q1.Lookup().Expect("[[DECIMAL(8)]]")

	g.TestExecute("DELETE FROM num")

	q0.Lookup().Expect("[]")
	q1.Lookup().Expect("[[DECIMAL(0)]]")
}

func TestAggregationNegativeSumsToZero(t *testing.T) {
	const Recipe = `
       CREATE TABLE num ( pk BIGINT NOT NULL AUTO_INCREMENT, b INT, a INT, PRIMARY KEY(pk));
       SELECT sum(a), b FROM num GROUP BY b;
       SELECT sum(a) FROM num;
`

	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 4; i++ {
		g.TestExecute("INSERT INTO num (a, b) VALUES (%d, %d)", i, i*10)
	}

	q0 := g.View("q0")
	q0.Lookup().Expect("[[DECIMAL(4) INT32(40)] [DECIMAL(3) INT32(30)] [DECIMAL(2) INT32(20)] [DECIMAL(1) INT32(10)]]")

	q1 := g.View("q1")
	q1.Lookup().Expect("[[DECIMAL(10)]]")

	g.TestExecute("INSERT INTO num (a, b) VALUES(-3, 30)")

	q0.Lookup().Expect("[[DECIMAL(4) INT32(40)] [DECIMAL(0) INT32(30)] [DECIMAL(2) INT32(20)] [DECIMAL(1) INT32(10)]]")
	q1.Lookup().Expect("[[DECIMAL(7)]]")

	g.TestExecute("DELETE FROM num WHERE b = 20")

	q0.Lookup().Expect("[[DECIMAL(4) INT32(40)] [DECIMAL(0) INT32(30)] [DECIMAL(1) INT32(10)]]")
	q1.Lookup().Expect("[[DECIMAL(5)]]")
}

func TestAggregationWithUpdate(t *testing.T) {
	const Recipe = `
       CREATE TABLE num ( pk BIGINT NOT NULL AUTO_INCREMENT, b INT, a INT, PRIMARY KEY(pk));
       SELECT count(a), b FROM num WHERE b = :b GROUP BY b;
`

	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 4; i++ {
		g.TestExecute("INSERT INTO num (a, b) VALUES (%d, %d)", i, i*10)
	}

	q0 := g.View("q0")
	q0.Lookup(20).Expect("[[INT64(1) INT32(20)]]")

	g.TestExecute("UPDATE num SET b = 100 WHERE b = 20")

	q0.Lookup(20).Expect("[]")
	q0.Lookup(100).Expect("[[INT64(1) INT32(100)]]")
}

func TestAggregationPartiallyOverlappingUpdate(t *testing.T) {
	const Recipe = `
CREATE TABLE votes (aid INT(32), uid INT(32), comment_id INT(32), sign INT(32),  PRIMARY KEY (aid, uid));
select count(*), aid from votes where uid = :v1 group by aid;
`

	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for aid := 1; aid <= 4; aid++ {
		for uid := 1; uid <= 4; uid++ {
			g.TestExecute("INSERT INTO votes (aid, uid, comment_id, sign) VALUES (%d, %d, %d, 1)", aid, uid, rand.Intn(100))
		}
	}

	q0 := g.View("q0")
	q0.Lookup(3).Expect("[[INT64(1) INT32(1)] [INT64(1) INT32(2)] [INT64(1) INT32(3)] [INT64(1) INT32(4)]]")

	g.TestExecute("UPDATE votes SET aid = 5 WHERE aid = 1")

	q0.Lookup(3).Expect("[[INT64(1) INT32(2)] [INT64(1) INT32(3)] [INT64(1) INT32(4)] [INT64(1) INT32(5)]]")
}

func TestAggregationPartiallyOverlappingNoMidflow(t *testing.T) {
	const Recipe = `
CREATE TABLE votes (aid INT(32), uid INT(32), comment_id INT(32), sign INT(32),  PRIMARY KEY (aid, uid));
select count(*), aid from votes where uid = :v1 group by aid;
`

	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithCustomBoostConfig(func(cfg *config.Config) {
		cfg.Materialization.UpqueryMode = config.UpqueryGenerationMode_NO_MIDFLOW_UPQUERIES
	}))

	err := g.TryApplyRecipe(recipe)
	require.Error(t, err)
}

func TestAggregationPartiallyOverlappingInsert(t *testing.T) {
	const Recipe = `
CREATE TABLE votes (aid INT(32), uid INT(32), comment_id INT(32), sign INT(32),  PRIMARY KEY (aid, uid));
select count(*), aid from votes where uid = :v1 group by aid, uid;
`

	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for aid := 1; aid <= 4; aid++ {
		g.TestExecute("INSERT INTO votes (aid, uid, comment_id, sign) VALUES (%d, %d, %d, 1)", aid, 3, rand.Intn(100))
	}

	q0 := g.View("q0")
	q0.Lookup(3).Expect("[[INT64(1) INT32(1)] [INT64(1) INT32(2)] [INT64(1) INT32(3)] [INT64(1) INT32(4)]]")

	g.TestExecute("INSERT INTO votes (aid, uid, comment_id, sign) VALUES (%d, %d, %d, 1)", 5, 3, rand.Intn(100))

	q0.Lookup(3).Expect("[[INT64(1) INT32(1)] [INT64(1) INT32(2)] [INT64(1) INT32(3)] [INT64(1) INT32(4)] [INT64(1) INT32(5)]]")
}

func TestPostFilterWithAggregation(t *testing.T) {
	const Recipe = `
	CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT, a INT, b INT, c INT, PRIMARY KEY(pk));
	select count(*) from num as n where n.a = :v1 and b not in ::v2 group by n.b;
	select count(*), num.b from num where num.b >= ? and num.b <= ? group by num.b;
	select count(*), sum(n.c), min(n.c), max(n.c) from num as n where n.a = :v1 and b not in ::v2;
	select count(*), sum(n.c) from num as n where n.a = :v1 and b not in ::v2 and c not in ::v3 group by n.b;
	select count(*) from num where a = :v1 and c not in ::v2 group by b;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 0; i < 8; i++ {
		g.TestExecute(`INSERT INTO num (a, b, c) VALUES (%d, %d, %d)`, i%2, i%3, i%5)
	}

	// 0, 0, 0
	// 1, 1, 1
	// 0, 2, 2
	// 1, 0, 3
	// 0, 1, 4
	// 1, 2, 0
	// 0, 0, 1
	// 1, 1, 2

	g.View("q0").LookupBvar(0, []any{0, 420, 69}).Expect(`[[INT64(1)] [INT64(1)]]`)
	g.View("q1").LookupBvar(1, 2).Expect(`[[INT64(3) INT32(1)] [INT64(2) INT32(2)]]`)
	g.View("q2").LookupBvar(0, []any{0, 420, 69}).Expect(`[[INT64(2) DECIMAL(6) INT32(2) INT32(4)]]`)
	g.View("q3").LookupBvar(0, []any{0, 420, 69}, []any{3, 4}).Expect(`[[INT64(1) DECIMAL(2)]]`)
	g.View("q3").LookupBvar(0, []any{0, 420, 69}, []any{420}).Expect(`[[INT64(1) DECIMAL(2)] [INT64(1) DECIMAL(4)]]`)
	g.View("q4").LookupBvar(1, []any{0, 420, 69}).Expect(`[[INT64(2)] [INT64(1)]]`)
}

func TestCountStarWithFunctionInWhereClause(t *testing.T) {
	const Recipe = `
	CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT, b INT, a INT, txt VARCHAR(255), PRIMARY KEY(pk));
SELECT count(*) FROM num WHERE LOWER(txt) = ?;`

	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 3; i++ {
		g.TestExecute("INSERT INTO num (a, b, txt) VALUES (%d, 100 * %d, 'hey')", i, i)
	}
	g.TestExecute("INSERT INTO num (a, b, txt) VALUES (null, 100, 'hey')")

	g.View("q0").Lookup("hey").Expect(`[[INT64(4)]]`)
}

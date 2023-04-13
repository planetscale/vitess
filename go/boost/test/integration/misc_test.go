package integration

import (
	"testing"

	"vitess.io/vitess/go/boost/test/helpers/boosttest"
	"vitess.io/vitess/go/boost/test/helpers/boosttest/testexecutor"
	"vitess.io/vitess/go/boost/test/helpers/boosttest/testrecipe"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

func SetupExternal(t *testing.T, options ...boosttest.Option) *boosttest.Cluster {
	t.Cleanup(func() {
		boosttest.EnsureNoLeaks(t)
	})

	ts := memorytopo.NewServer(boosttest.DefaultLocalCell)
	executor := testexecutor.Default(t)
	executor.TestUpdateTopoServer(ts, boosttest.DefaultLocalCell)

	var allOptions []boosttest.Option
	allOptions = append(allOptions, boosttest.WithTopoServer(ts), boosttest.WithFakeExecutor(executor), boosttest.WithShards(0))
	allOptions = append(allOptions, options...)

	return boosttest.New(t, allOptions...)
}

func TestStarIsOK(t *testing.T) {
	const Recipe = `
	CREATE TABLE num (
	    pk BIGINT NOT NULL AUTO_INCREMENT, 
		b INT,
	    a INT,
	PRIMARY KEY(pk));

	SELECT * FROM num;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	_ = SetupExternal(t, boosttest.WithTestRecipe(recipe))
}

func TestHaving(t *testing.T) {
	const Recipe = `
CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT,
	a INT, b SMALLINT,
	PRIMARY KEY(pk));

SELECT a, SUM(b) FROM num GROUP BY a HAVING a = 2 AND SUM(b) > 10;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 3; i++ {
		/*
			pk | a | b
			0  | 1 | 1
			1  | 2 | 1
			2  | 1 | 2
			3  | 2 | 4
			4  | 1 | 3
			3  | 2 | 9
		*/
		g.TestExecute("INSERT INTO num (a, b) VALUES (1, %d), (2, %d)", i, i*i)
	}

	g.View("q0").Lookup().Expect(`[[INT32(2) DECIMAL(14)]]`)
}

func TestDistinct(t *testing.T) {
	const Recipe = `
CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT,
	a INT, 
	b INT,
	PRIMARY KEY(pk));

	SELECT /*vt+ VIEW=distinct */ DISTINCT a, b FROM num;
	SELECT /*vt+ VIEW=distinct2 */ DISTINCT a, b FROM num WHERE b = ?;
    SELECT /*vt+ VIEW=union_distinct */ a, b FROM num UNION DISTINCT SELECT a, b FROM num;  
    SELECT /*vt+ VIEW=union_distinct2 */ a, 10 FROM num UNION DISTINCT SELECT b, 20 FROM num;  
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))
	for i := 1; i <= 3; i++ {
		g.TestExecute("INSERT INTO num (a, b) VALUES (%d, %d), (%d, %d)", i, i*i, i, i*i)
	}

	g.View("distinct").Lookup().Expect(`[[INT32(3) INT32(9)] [INT32(2) INT32(4)] [INT32(1) INT32(1)]]`)
	g.View("union_distinct").Lookup().Expect(`[[INT32(3) INT32(9)] [INT32(2) INT32(4)] [INT32(1) INT32(1)]]`)
	g.View("union_distinct2").Lookup().Expect(`[[INT32(1) INT64(20)] [INT32(1) INT64(10)] [INT32(4) INT64(20)] [INT32(2) INT64(10)] [INT32(9) INT64(20)] [INT32(3) INT64(10)]]`)
	g.View("distinct2").Lookup(int32(4)).Expect(`[[INT32(2) INT32(4)]]`)

	g.TestExecute("INSERT INTO num (a, b) VALUES (5, 4)")

	g.View("distinct").Lookup().Expect(`[[INT32(5) INT32(4)] [INT32(3) INT32(9)] [INT32(2) INT32(4)] [INT32(1) INT32(1)]]`)
	g.View("union_distinct").Lookup().Expect(`[[INT32(5) INT32(4)] [INT32(3) INT32(9)] [INT32(2) INT32(4)] [INT32(1) INT32(1)]]`)
	g.View("distinct2").Lookup(int32(4)).Expect(`[[INT32(2) INT32(4)] [INT32(5) INT32(4)]]`)
}

func TestTypeConversions(t *testing.T) {
	const Recipe = `
	CREATE TABLE conv (pk BIGINT NOT NULL AUTO_INCREMENT,
	col_int INT, col_double DOUBLE, col_decimal DECIMAL, col_char VARCHAR(255),
	PRIMARY KEY(pk));

	SELECT conv.pk FROM conv WHERE conv.col_int = :x;
	SELECT conv.pk FROM conv WHERE conv.col_double = :x;
	SELECT conv.pk FROM conv WHERE conv.col_char = :x;
	SELECT conv.pk FROM conv WHERE conv.col_char IN ::x;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 3; i++ {
		g.TestExecute("INSERT INTO conv (col_int, col_double, col_decimal, col_char) VALUES (%d, %f, %f, 'string-%d')", i, float64(i), float64(i), i)
	}

	conv0 := g.View("q0")
	conv0.Lookup("1").Expect(`[[INT64(1)]]`)
	conv0.Lookup("1.0").Expect(`[[INT64(1)]]`)
	conv0.Lookup(1.0).Expect(`[[INT64(1)]]`)
	conv0.Lookup("1.5").Expect(`[]`)
	conv0.Lookup(1.5).Expect(`[]`)

	conv1 := g.View("q1")
	conv1.Lookup("1").Expect(`[[INT64(1)]]`)
	conv1.Lookup("1.0").Expect(`[[INT64(1)]]`)
	conv1.Lookup(1.0).Expect(`[[INT64(1)]]`)
	conv1.Lookup("1.5").Expect(`[]`)
	conv1.Lookup(1.5).Expect(`[]`)

	conv2 := g.View("q2")
	conv2.Lookup("string-1").Expect(`[[INT64(1)]]`)
	conv2.Lookup(1).ExpectError()
	conv2.Lookup(1.0).ExpectError()

	conv3 := g.View("q3")
	conv3.LookupBvar([]any{"string-1", "string-2"}).ExpectLen(2)
	conv3.LookupBvar([]any{"string-1", 1}).ExpectError()
}

func TestDeletedReader(t *testing.T) {
	const Recipe = `
	CREATE TABLE tbl (pk BIGINT NOT NULL AUTO_INCREMENT,
	a BIGINT, b BIGINT, c BIGINT,
	PRIMARY KEY(pk));

	SELECT tbl.pk FROM tbl WHERE tbl.a = :a and tbl.b = :b and tbl.c = :c;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 16; i++ {
		g.TestExecute("INSERT INTO tbl (a, b, c) VALUES (%d, %d, %d)", i, i%4, i*10)
	}

	upquery0 := g.View("q0")
	upquery0.Lookup(1, 1, 10).Expect(`[[INT64(1)]]`)

	g.ApplyRecipe(&testrecipe.Recipe{})

	upquery0.Lookup(1, 1, 10).ExpectErrorEventually()
}

func TestNoKeyspaceRouting(t *testing.T) {
	const Recipe = `
	CREATE TABLE tbl (pk BIGINT NOT NULL AUTO_INCREMENT,
	a BIGINT, b BIGINT, c BIGINT,
	PRIMARY KEY(pk));

	SELECT tbl.pk FROM tbl WHERE tbl.a = :a and tbl.b = :b and tbl.c = :c;
`
	recipe := testrecipe.NewRecipeFromSQL(t, testrecipe.DefaultKeyspace, "", Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 16; i++ {
		g.TestExecute("INSERT INTO tbl (a, b, c) VALUES (%d, %d, %d)", i, i%4, i*10)
	}

	upquery0 := g.View("q0")
	upquery0.Lookup(1, 1, 10).Expect(`[[INT64(1)]]`)

	g.ApplyRecipe(&testrecipe.Recipe{})

	upquery0.Lookup(1, 1, 10).ExpectErrorEventually()
}

func TestRewriteOrderByColumnQualifier(t *testing.T) {
	const Recipe = `
create table t1 (
       id int not null auto_increment,
       PRIMARY KEY (id)
) ENGINE InnoDB,
  CHARSET utf8mb4,
  COLLATE utf8mb4_0900_ai_ci;

select alias.id from t1 as alias where alias.id = 42 order by alias.id desc
`
	recipe := testrecipe.NewRecipeFromSQL(t, testrecipe.DefaultKeyspace, "", Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	upquery0 := g.View("q0")
	upquery0.Lookup().Expect(`[]`)
}

package integration

import (
	"context"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/common/xslice"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/server/controller"
	"vitess.io/vitess/go/boost/server/worker"
	"vitess.io/vitess/go/boost/test/helpers/boosttest"
	"vitess.io/vitess/go/boost/test/helpers/boosttest/testexecutor"
	"vitess.io/vitess/go/boost/test/helpers/boosttest/testrecipe"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/vtboost"
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

func TestMaterializationWithIgnoredPackets(t *testing.T) {
	const Recipe = `
CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT, a BIGINT, PRIMARY KEY(pk));
SELECT /*vt+ VIEW=v_partial PUBLIC */ num.a FROM num WHERE num.pk = :primary;
SELECT /*vt+ VIEW=v_full PUBLIC */ SUM(num.a) FROM num;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe), boosttest.WithFakeExecutorLatency(100*time.Millisecond))

	for i := 1; i <= 3; i++ {
		g.TestExecute("INSERT INTO num (a) VALUES (%d)", i)
	}

	g.View("v_partial").Lookup().Expect([]sqltypes.Row{})

	g.View("v_partial").Lookup(1).Expect([]sqltypes.Row{{sqltypes.NewInt64(1)}})
	time.Sleep(500 * time.Millisecond)
	g.View("v_full").Lookup().Expect([]sqltypes.Row{{sqltypes.NewDecimal("6")}})
}

func TestMaterializationWithIgnoredPacketsAndMultiplePartial(t *testing.T) {
	const Recipe = `
CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT, a BIGINT, b BIGINT, PRIMARY KEY(pk));
SELECT /*vt+ VIEW=v_sum_a PUBLIC */ SUM(num.a) FROM num WHERE num.b = :b;
SELECT /*vt+ VIEW=v_sum_b PUBLIC */ SUM(num.b) FROM num WHERE num.a = :a;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe), boosttest.WithFakeExecutorLatency(100*time.Millisecond))

	g.TestExecute("INSERT INTO num (a, b) VALUES (%d, %d)", 1, 2)
	g.TestExecute("INSERT INTO num (a, b) VALUES (%d, %d)", 1, 3)
	g.TestExecute("INSERT INTO num (a, b) VALUES (%d, %d)", 2, 2)
	g.TestExecute("INSERT INTO num (a, b) VALUES (%d, %d)", 2, 3)

	g.View("v_sum_b").Lookup(2).Expect([]sqltypes.Row{{sqltypes.NewDecimal("5")}})

	time.Sleep(50 * time.Millisecond)
	g.TestExecute("INSERT INTO num (a, b) VALUES (%d, %d)", 2, 3)
	g.TestExecute("INSERT INTO num (a, b) VALUES (%d, %d)", 3, 2)
	g.TestExecute("INSERT INTO num (a, b) VALUES (%d, %d)", 3, 3)

	time.Sleep(200 * time.Millisecond)
	g.View("v_sum_a").Lookup(2).Expect([]sqltypes.Row{{sqltypes.NewDecimal("6")}})

	time.Sleep(500 * time.Millisecond)
	g.View("v_sum_b").Lookup(2).Expect([]sqltypes.Row{{sqltypes.NewDecimal("8")}})
}

func TestProjections(t *testing.T) {
	const Recipe = `
	CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT,
	a INT, b SMALLINT, c TINYINT, d MEDIUMINT, e BIGINT,
	f FLOAT, g DOUBLE, h DECIMAL,
	PRIMARY KEY(pk));

	SELECT /*vt+ VIEW=op0 PUBLIC */ num.a, num.a + num.b, 420 FROM num WHERE num.a = :x;
	SELECT /*vt+ VIEW=op1 PUBLIC */ num.a, num.a + num.b, 420 FROM num;
	SELECT /*vt+ VIEW=op3 PUBLIC */ col1 + col2, 420 FROM (SELECT a as col1, a+b as col2 FROM num) apa WHERE col1 = :x;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 3; i++ {
		g.TestExecute("INSERT INTO num (a, b, c, d, e, f, g, h) VALUES (%d, %d, %d, %d, %d, %d, %d, %d)", i, i, i, i, i, i, i, i)
	}

	g.View("op0").Lookup(1).Expect([]sqltypes.Row{
		{sqltypes.NewInt32(1), sqltypes.NewInt64(2), sqltypes.NewInt64(420)},
	})

	g.View("op0").Lookup("1").Expect([]sqltypes.Row{
		{sqltypes.NewInt32(1), sqltypes.NewInt64(2), sqltypes.NewInt64(420)},
	})

	g.View("op1").Lookup().Expect([]sqltypes.Row{
		{sqltypes.NewInt32(1), sqltypes.NewInt64(2), sqltypes.NewInt64(420)},
		{sqltypes.NewInt32(2), sqltypes.NewInt64(4), sqltypes.NewInt64(420)},
		{sqltypes.NewInt32(3), sqltypes.NewInt64(6), sqltypes.NewInt64(420)},
	})

	g.View("op3").Lookup(1).Expect([]sqltypes.Row{
		{sqltypes.NewInt64(3), sqltypes.NewInt64(420)},
	})
}

func TestProjectionsWithMigration(t *testing.T) {
	const Recipe = `
	CREATE TABLE num (
	    pk BIGINT NOT NULL AUTO_INCREMENT, 
	    a INT, 
	    b SMALLINT, 
	    PRIMARY KEY(pk));

	SELECT /*vt+ VIEW=op0 PUBLIC */ num.a, num.a + num.b, 420 FROM num WHERE num.a = :x;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 3; i++ {
		g.TestExecute("INSERT INTO num (a, b) VALUES (%d, %d)", i, i)
	}

	g.View("op0").Lookup(1).Expect([]sqltypes.Row{
		{sqltypes.NewInt32(1), sqltypes.NewInt64(2), sqltypes.NewInt64(420)},
	})

	g.AlterRecipe(recipe, "alter table num add column c bigint")

	recipe.Queries = append(recipe.Queries, &vtboost.CachedQuery{
		Name:     "op1",
		Sql:      "SELECT num.c, num.a + num.b, 420 FROM num WHERE num.a = :x",
		Keyspace: testrecipe.DefaultKeyspace,
	})

	g.ApplyRecipe(recipe)

	g.TestExecute("INSERT INTO num (a, b, c) VALUES (%d, %d, %d)", 10, 10, 666)
	boosttest.Settle()

	g.View("op0").Lookup(1).Expect([]sqltypes.Row{
		{sqltypes.NewInt32(1), sqltypes.NewInt64(2), sqltypes.NewInt64(420)},
	})
	g.View("op1").Lookup(10).Expect([]sqltypes.Row{
		{sqltypes.NewInt64(666), sqltypes.NewInt64(20), sqltypes.NewInt64(420)},
	})
}

func TestAggregations(t *testing.T) {
	const Recipe = `
	CREATE TABLE num (
	    pk BIGINT NOT NULL AUTO_INCREMENT, 
		b INT,
	    a INT,
	PRIMARY KEY(pk));

	SELECT /*vt+ VIEW=op0 PUBLIC */ count(num.a), count(*) FROM num;
	SELECT /*vt+ VIEW=op1 PUBLIC */ b, count(num.a), count(*) FROM num GROUP BY b;
	SELECT /*vt+ VIEW=op2 PUBLIC */ count(num.a), count(*) FROM num WHERE a = ?;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 3; i++ {
		g.TestExecute("INSERT INTO num (a, b) VALUES (%d, 100 * %d)", i, i)
	}
	g.TestExecute("INSERT INTO num (a, b) VALUES (null, 100)")

	g.View("op0").Lookup().Expect([]sqltypes.Row{
		{sqltypes.NewInt64(3), sqltypes.NewInt64(4)},
	})
	g.View("op1").Lookup().Expect([]sqltypes.Row{
		{sqltypes.NewInt32(100), sqltypes.NewInt64(1), sqltypes.NewInt64(2)},
		{sqltypes.NewInt32(200), sqltypes.NewInt64(1), sqltypes.NewInt64(1)},
		{sqltypes.NewInt32(300), sqltypes.NewInt64(1), sqltypes.NewInt64(1)},
	})
	g.View("op2").Lookup(2).Expect([]sqltypes.Row{
		{sqltypes.NewInt64(1), sqltypes.NewInt64(1)},
	})
}

func TestAggregationsWithFullExternalReplay(t *testing.T) {
	const Recipe = `
	CREATE TABLE num (
	    pk BIGINT NOT NULL AUTO_INCREMENT,
		b INT,
	    a INT,
	PRIMARY KEY(pk));

	SELECT /*vt+ VIEW=op0 PUBLIC */ count(num.a), count(*) FROM num;
	SELECT /*vt+ VIEW=op1 PUBLIC */ b, count(num.a), count(*) FROM num GROUP BY b;
	SELECT /*vt+ VIEW=op2 PUBLIC */ count(num.a), count(*) FROM num WHERE a = ?;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t)
	g.ApplyRecipeEx(recipe, true, false)

	for i := 1; i <= 3; i++ {
		g.TestExecute("INSERT INTO num (a, b) VALUES (%d, 100 * %d)", i, i)
	}
	g.TestExecute("INSERT INTO num (a, b) VALUES (null, 100)")

	time.Sleep(100 * time.Millisecond)
	g.ApplyRecipeEx(recipe, false, true)

	g.View("op0").Lookup().Expect([]sqltypes.Row{
		{sqltypes.NewInt64(3), sqltypes.NewInt64(4)},
	})
	g.View("op1").Lookup().Expect([]sqltypes.Row{
		{sqltypes.NewInt32(100), sqltypes.NewInt64(1), sqltypes.NewInt64(2)},
		{sqltypes.NewInt32(200), sqltypes.NewInt64(1), sqltypes.NewInt64(1)},
		{sqltypes.NewInt32(300), sqltypes.NewInt64(1), sqltypes.NewInt64(1)},
	})
	g.View("op2").Lookup(2).Expect([]sqltypes.Row{
		{sqltypes.NewInt64(1), sqltypes.NewInt64(1)},
	})
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

func TestCustomBaseTypes(t *testing.T) {
	const Recipe = `
CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT,
	a INT, b SMALLINT, c TINYINT, d MEDIUMINT, e BIGINT,
	f FLOAT, g DOUBLE, h DECIMAL,
	PRIMARY KEY(pk));

	SELECT /*vt+ VIEW=identity PUBLIC */ num.a, num.b, num.c, num.d, num.e, num.f, num.g, num.h
	FROM num WHERE num.pk = :primary;

	SELECT /*vt+ VIEW=summed_g PUBLIC */ SUM(num.g) FROM num;
	
	SELECT /*vt+ VIEW=max_a PUBLIC */ MAX(num.a) FROM num;
	SELECT /*vt+ VIEW=max_g PUBLIC */ MAX(num.g) FROM num;
	
	SELECT /*vt+ VIEW=min_a PUBLIC */ MIN(num.a) FROM num;
	SELECT /*vt+ VIEW=min_g PUBLIC */ MIN(num.g) FROM num;
	SELECT /*vt+ VIEW=summed_a PUBLIC */ SUM(num.a) FROM num;
	SELECT /*vt+ VIEW=multi PUBLIC */ SUM(num.a), MIN(num.g) FROM num;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))
	g.Executor.SetMaxBatchSize(1)

	for i := 1; i <= 3; i++ {
		g.TestExecute("INSERT INTO num (a, b, c, d, e, f, g, h) VALUES (%d, %d, %d, %d, %d, %d, %d, %d)", i, i, i, i, i, i, i, i)
	}

	identity := g.View("identity").Lookup(1).ExpectLen(1)
	t.Logf("lookup identity: %v", identity.Rows)

	g.View("summed_a").Lookup().Expect([]sqltypes.Row{{sqltypes.NewDecimal("6")}})
	g.View("summed_g").Lookup().Expect([]sqltypes.Row{{sqltypes.NewFloat64(6)}})

	g.View("max_a").Lookup().Expect([]sqltypes.Row{{sqltypes.NewInt32(3)}})
	g.View("max_g").Lookup().Expect([]sqltypes.Row{{sqltypes.NewFloat64(3)}})

	g.View("min_a").Lookup().Expect([]sqltypes.Row{{sqltypes.NewInt32(1)}})
	g.View("min_g").Lookup().Expect([]sqltypes.Row{{sqltypes.NewFloat64(1)}})
	g.View("multi").Lookup().Expect([]sqltypes.Row{{sqltypes.NewDecimal("6"), sqltypes.NewFloat64(1)}})
}

func TestHaving(t *testing.T) {
	const Recipe = `
CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT,
	a INT, b SMALLINT,
	PRIMARY KEY(pk));

SELECT /*vt+ VIEW=summed PUBLIC */ a, SUM(b) FROM num GROUP BY a HAVING a = 2 AND SUM(b) > 10;
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

	g.View("summed").Lookup().Expect([]sqltypes.Row{{sqltypes.NewInt32(2), sqltypes.NewDecimal("14")}})
}

func TestTopK(t *testing.T) {
	const Recipe = `
CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT,
	a INT, 
	b INT,
	PRIMARY KEY(pk));

	SELECT /*vt+ VIEW=top PUBLIC */ a, b FROM num WHERE a = ? ORDER BY b LIMIT 3;
	SELECT /*vt+ VIEW=top_with_bogokey PUBLIC */ a, b FROM num ORDER BY b LIMIT 3;
	SELECT /*vt+ VIEW=top_without_key PUBLIC */ pk FROM num WHERE a = ? ORDER BY b LIMIT 3;
	SELECT /*vt+ VIEW=top_multi PUBLIC */ pk FROM num WHERE a IN ::a ORDER BY b LIMIT 4;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 5; i++ {
		/*
			pk | a | b
			1  | 1 | 1
			2  | 2 | -1
			3  | 1 | 2
			4  | 2 | -4
			5  | 1 | 3
			6  | 2 | -9
			7  | 1 | 4
			8  | 2 | -16
			9  | 1 | 5
			10 | 2 | -25
		*/
		g.TestExecute("INSERT INTO num (a, b) VALUES (1, %d), (2, -%d)", i, i*i)
	}

	g.View("top").Lookup(int32(2)).ExpectSorted(
		[]sqltypes.Row{
			{sqltypes.NewInt32(2), sqltypes.NewInt32(-25)},
			{sqltypes.NewInt32(2), sqltypes.NewInt32(-16)},
			{sqltypes.NewInt32(2), sqltypes.NewInt32(-9)},
		})

	g.View("top").Lookup(int32(1)).ExpectSorted(
		[]sqltypes.Row{
			{sqltypes.NewInt32(1), sqltypes.NewInt32(1)},
			{sqltypes.NewInt32(1), sqltypes.NewInt32(2)},
			{sqltypes.NewInt32(1), sqltypes.NewInt32(3)},
		})

	g.View("top_with_bogokey").Lookup().ExpectSorted(
		[]sqltypes.Row{
			{sqltypes.NewInt32(2), sqltypes.NewInt32(-25)},
			{sqltypes.NewInt32(2), sqltypes.NewInt32(-16)},
			{sqltypes.NewInt32(2), sqltypes.NewInt32(-9)},
		})

	g.View("top_without_key").Lookup(int32(2)).ExpectSorted(
		[]sqltypes.Row{
			{sqltypes.NewInt64(10)},
			{sqltypes.NewInt64(8)},
			{sqltypes.NewInt64(6)},
		})

	g.View("top_multi").LookupBvar([]any{1, 2}).ExpectSorted(
		[]sqltypes.Row{
			{sqltypes.NewInt64(10)},
			{sqltypes.NewInt64(8)},
			{sqltypes.NewInt64(6)},
			{sqltypes.NewInt64(4)},
		})
}

func TestDistinct(t *testing.T) {
	const Recipe = `
CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT,
	a INT, 
	b INT,
	PRIMARY KEY(pk));

	SELECT /*vt+ VIEW=distinct PUBLIC */ DISTINCT a, b FROM num;
	SELECT /*vt+ VIEW=distinct2 PUBLIC */ DISTINCT a, b FROM num WHERE b = ?;
    SELECT /*vt+ VIEW=union_distinct PUBLIC */ a, b FROM num UNION DISTINCT SELECT a, b FROM num; 
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 3; i++ {
		g.TestExecute("INSERT INTO num (a, b) VALUES (%d, %d), (%d, %d)", i, i*i, i, i*i)
	}

	expected := []sqltypes.Row{
		{sqltypes.NewInt32(3), sqltypes.NewInt32(9)},
		{sqltypes.NewInt32(2), sqltypes.NewInt32(4)},
		{sqltypes.NewInt32(1), sqltypes.NewInt32(1)},
	}
	g.View("distinct").Lookup().Expect(expected)
	g.View("union_distinct").Lookup().Expect(expected)

	g.View("distinct2").Lookup(int32(4)).Expect(
		[]sqltypes.Row{
			{sqltypes.NewInt32(2), sqltypes.NewInt32(4)},
		})

	g.TestExecute("INSERT INTO num (a, b) VALUES (5, 4)")
	time.Sleep(1 * time.Second)

	expected = []sqltypes.Row{
		{sqltypes.NewInt32(5), sqltypes.NewInt32(4)},
		{sqltypes.NewInt32(3), sqltypes.NewInt32(9)},
		{sqltypes.NewInt32(2), sqltypes.NewInt32(4)},
		{sqltypes.NewInt32(1), sqltypes.NewInt32(1)},
	}
	g.View("distinct").Lookup().Expect(expected)
	g.View("union_distinct").Lookup().Expect(expected)

	g.View("distinct2").Lookup(int32(4)).Expect(
		[]sqltypes.Row{
			{sqltypes.NewInt32(2), sqltypes.NewInt32(4)},
			{sqltypes.NewInt32(5), sqltypes.NewInt32(4)},
		})
}

func TestAggregation(t *testing.T) {
	queries := `
CREATE TABLE vote (id bigint NOT NULL AUTO_INCREMENT, article_id bigint, user bigint, PRIMARY KEY(id));

SELECT /*vt+ VIEW=caseaggr PUBLIC */ user, sum(CASE WHEN article_id = 5 THEN 1 ELSE 0 END) AS sum FROM vote GROUP BY user
`
	recipe := testrecipe.LoadSQL(t, queries)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	g.TestExecute("INSERT INTO vote(id, article_id, user) VALUES (1, 1, 1), (2, 5, 2), (3, 2, 2), (4, 5, 2)")

	g.View("caseaggr").Lookup().Expect([]sqltypes.Row{
		{sqltypes.NewInt64(2), sqltypes.NewDecimal("2")},
		{sqltypes.NewInt64(1), sqltypes.NewDecimal("0")},
	})
}

func TestBasicEx(t *testing.T) {
	recipe := testrecipe.Load(t, "basic")
	ts := memorytopo.NewServer(boosttest.DefaultLocalCell)

	executor := testexecutor.Default(t)
	executor.TestApplyRecipe(recipe)
	executor.TestUpdateTopoServer(ts, boosttest.DefaultLocalCell)

	g := boosttest.New(t, boosttest.WithTopoServer(ts), boosttest.WithFakeExecutor(executor), boosttest.WithShards(0), boosttest.WithInstances(4))

	err := g.Controller().Migrate(context.Background(), func(_ context.Context, mig *controller.Migration) error {
		schema := boostpb.TestSchema(sqltypes.Int64, sqltypes.Int64)
		a := mig.AddBase("a", []string{"c1", "c2"}, flownode.NewExternalBase([]int{0}, schema, executor.Keyspace()))
		b := mig.AddBase("b", []string{"c1", "c2"}, flownode.NewExternalBase([]int{0}, schema, executor.Keyspace()))

		emits := map[graph.NodeIdx][]int{
			a: {0, 1},
			b: {0, 1},
		}
		u := flownode.NewUnion(emits)
		c := mig.AddIngredient("c", []string{"c1", "c2"}, u)
		mig.MaintainAnonymous(c, []int{0})
		return nil
	})
	require.NoError(t, err, "migration failed")

	var id = sqltypes.NewInt64(1)
	cq := g.View("c")

	executor.TestExecute("INSERT INTO a VALUES (1, 2)")
	cq.Lookup(id).Expect([]sqltypes.Row{{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}})

	executor.TestExecute("INSERT INTO b VALUES (1, 4)")
	cq.Lookup(id).Expect([]sqltypes.Row{
		{id, sqltypes.NewInt64(2)},
		{id, sqltypes.NewInt64(4)},
	})

	executor.TestExecute("DELETE FROM a WHERE c1 = 1")
	cq.Lookup(id).Expect([]sqltypes.Row{
		{id, sqltypes.NewInt64(4)},
	})

	executor.TestExecute("UPDATE b SET c2 = 3 WHERE c1 = 1")
	cq.Lookup(id).Expect([]sqltypes.Row{
		{id, sqltypes.NewInt64(3)},
	})
}

func TestVoteRecipe(t *testing.T) {
	t.Run("Default", func(t *testing.T) {
		testVoteRecipe(t, testrecipe.Load(t, "votes"))
	})

	t.Run("JoinMismatch", func(t *testing.T) {
		recipesql := strings.Replace(testrecipe.Schema(t, "votes"), "article_id bigint", "article_id int", 1)
		testVoteRecipe(t, testrecipe.LoadSQL(t, recipesql))
	})
}

func testVoteRecipe(t *testing.T, recipe *testrecipe.Recipe) {
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	awvc := g.View("articlewithvotecount")

	const articleCount = 10
	for i := 0; i < articleCount; i++ {
		g.TestExecute("INSERT INTO `article` (`id`, `title`) VALUES (%d, 'Article %d')", i+1, i+1)
	}

	const userCount = 10
	const votesCount = 100
	for i := 0; i < votesCount; i++ {
		g.TestExecute("INSERT INTO `vote` (`article_id`, `user`) values (%d, %d)", (i%articleCount)+1, rand.Intn(userCount)+1)
	}

	time.Sleep(10 * time.Millisecond)

	awvc.Lookup(1).Expect([]sqltypes.Row{{sqltypes.NewInt64(1), sqltypes.NewVarChar("Article 1"), sqltypes.NewInt64(votesCount / articleCount)}})
	awvc.Lookup(2).Expect([]sqltypes.Row{{sqltypes.NewInt64(2), sqltypes.NewVarChar("Article 2"), sqltypes.NewInt64(votesCount / articleCount)}})

	require.Equal(t, 2, g.WorkerReads())
	require.Equal(t, articleCount+votesCount, g.WorkerStats(worker.StatVStreamRows))
}

func TestAlbumsRecipe(t *testing.T) {
	recipe := testrecipe.Load(t, "albums")
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	// four users: 1, 2, 3, and 4
	// 1 and 2 are friends, 3 is a friend of 1 but not 2
	// 4 isn't friends with anyone
	//
	// four albums: x, y, z, and q; one authored by each user
	// z is public.
	//
	// there's one photo in each album
	//
	// what should each user be able to see?
	//
	//  - 1 should be able to see albums x, y, and z
	//  - 2 should be able to see albums x, y, and z
	//  - 3 should be able to see albums x and z
	//  - 4 should be able to see albums z and q

	g.TestExecute("INSERT INTO `friend` (`usera`, `userb`) VALUES (%d, %d), (%d, %d)", 1, 2, 3, 1)
	g.TestExecute("INSERT INTO `album` (`a_id`, `u_id`, `public`) VALUES ('%s', %d, %d), ('%s', %d, %d), ('%s', %d, %d), ('%s', %d, %d)",
		"albumX", 1, 0,
		"albumY", 2, 0,
		"albumZ", 3, 1,
		"albumQ", 4, 0)

	g.TestExecute("INSERT INTO `photo` (`p_id`, `album`) VALUES ('%s', '%s'), ('%s', '%s'), ('%s', '%s'), ('%s', '%s')",
		"photoA", "albumX",
		"photoB", "albumY",
		"photoC", "albumZ",
		"photoD", "albumQ")

	time.Sleep(100 * time.Millisecond)

	view := g.View("album_friends")
	view.Lookup().Expect([]sqltypes.Row{
		{sqltypes.NewVarChar("albumQ"), sqltypes.NewInt32(4)},
		{sqltypes.NewVarChar("albumY"), sqltypes.NewInt32(1)},
		{sqltypes.NewVarChar("albumY"), sqltypes.NewInt32(2)},
		{sqltypes.NewVarChar("albumX"), sqltypes.NewInt32(2)},
		{sqltypes.NewVarChar("albumX"), sqltypes.NewInt32(3)},
		{sqltypes.NewVarChar("albumX"), sqltypes.NewInt32(1)},
	})

	private := g.View("private_photos")
	public := g.View("public_photos")

	get := func(userid int64, albumid string) []sqltypes.Row {
		resPriv := private.LookupByFields(map[string]sqltypes.Value{
			"friend_id": sqltypes.NewInt64(userid),
			"album_id":  sqltypes.NewVarChar(albumid),
		})
		resPub := public.LookupByFields(map[string]sqltypes.Value{
			"album_id": sqltypes.NewVarChar(albumid),
		})
		var rows []sqltypes.Row
		rows = append(rows, resPriv.Rows...)
		rows = append(rows, resPub.Rows...)
		return rows
	}

	assertAll := func() {
		assert.Len(t, get(1, "albumX"), 1)
		assert.Len(t, get(1, "albumY"), 1)
		assert.Len(t, get(1, "albumZ"), 1)
		assert.Len(t, get(1, "albumQ"), 0)

		assert.Len(t, get(2, "albumX"), 1)
		assert.Len(t, get(2, "albumY"), 1)
		assert.Len(t, get(2, "albumZ"), 1)
		assert.Len(t, get(2, "albumQ"), 0)

		assert.Len(t, get(3, "albumX"), 1)
		assert.Len(t, get(3, "albumY"), 0)
		assert.Len(t, get(3, "albumZ"), 1)
		assert.Len(t, get(3, "albumQ"), 0)

		assert.Len(t, get(4, "albumX"), 0)
		assert.Len(t, get(4, "albumY"), 0)
		assert.Len(t, get(4, "albumZ"), 1)
		assert.Len(t, get(4, "albumQ"), 1)
	}

	assertAll()

	_ = g.DebugEviction(false, map[string]int64{"public_photos": 128})
	time.Sleep(100 * time.Millisecond)

	assertAll()
	require.Equal(t, 10, g.WorkerStats(worker.StatVStreamRows))
}

func TestLoadingExternalRecipes(t *testing.T) {
	for _, schemaName := range []string{"lobsters-schema", "tpc-h", "tpc-w"} {
		t.Run(schemaName, func(t *testing.T) {
			t.Skip("not green yet")

			recipe := testrecipe.Load(t, schemaName)
			_ = SetupExternal(t, boosttest.WithTestRecipe(recipe))
		})
	}
}

func TestSQLRecipe(t *testing.T) {
	recipe := testrecipe.Load(t, "cars")
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	getter := g.View("CountCars")

	for i, brand := range []string{"Volvo", "Volvo", "Volkswagen"} {
		g.TestExecute("INSERT INTO `Car` (`id`, `brand`) values (%d, '%s')", i, brand)
	}

	getter.Lookup("Volvo").Expect([]sqltypes.Row{{sqltypes.NewInt64(2)}})

	require.Equal(t, 3, g.WorkerStats(worker.StatVStreamRows))
}

func TestDoubleShuffle(t *testing.T) {
	recipe := testrecipe.Load(t, "double-shuffle")
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	carPrice := g.View("CarPrice")
	g.TestExecute("INSERT INTO `Price` (`pid`, `price`) values (%d, %d)", 1, 100)
	g.TestExecute("INSERT INTO `Car` (`cid`, `pid`) values (%d, %d)", 1, 1)

	carPrice.Lookup(1).Expect([]sqltypes.Row{{sqltypes.NewInt32(1), sqltypes.NewInt32(100)}})
	carPrice.Lookup(int32(1)).Expect([]sqltypes.Row{{sqltypes.NewInt32(1), sqltypes.NewInt32(100)}})

	require.Equal(t, 2, g.WorkerStats(worker.StatVStreamRows))
}

func TestRecipeModifications(t *testing.T) {
	const Tables = `
CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT, a BIGINT, b BIGINT, PRIMARY KEY(pk));
`
	recipe := testrecipe.LoadSQL(t, Tables)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 3; i++ {
		g.TestExecute("INSERT INTO num (a) VALUES (%d)", i)
	}

	recipe.Queries = append(recipe.Queries, &vtboost.CachedQuery{
		PublicId: "deadbeef",
		Name:     "q1",
		Sql:      "SELECT a + 1 FROM num WHERE pk = :x",
		Keyspace: testrecipe.DefaultKeyspace,
	})

	g.ApplyRecipe(recipe)

	clientQ1 := g.View("q1")
	clientQ1.Lookup(1).Expect([]sqltypes.Row{{sqltypes.NewInt64(2)}})

	recipe.Queries = append(recipe.Queries, &vtboost.CachedQuery{
		PublicId: "c0ffe",
		Name:     "q2",
		Sql:      "SELECT a * 2 FROM num WHERE pk = :x",
		Keyspace: testrecipe.DefaultKeyspace,
	})

	g.ApplyRecipe(recipe)

	clientQ1.Lookup(1).Expect([]sqltypes.Row{{sqltypes.NewInt64(2)}})

	clientQ2 := g.View("q2")
	clientQ2.Lookup(1).Expect([]sqltypes.Row{{sqltypes.NewInt64(2)}})

	recipe.Queries = xslice.Filter(recipe.Queries, func(q *vtboost.CachedQuery) bool { return q.Name != "q1" })
	g.ApplyRecipe(recipe)

	time.Sleep(100 * time.Millisecond)

	require.Nil(t, g.FindView("q1"))
	clientQ2.Lookup(1).Expect([]sqltypes.Row{{sqltypes.NewInt64(2)}})
}

func TestFriendOfFriends(t *testing.T) {
	// Database of persons and dogs, and which people are friends with which dogs
	const Recipe = `
CREATE TABLE person (
    pid BIGINT NOT NULL AUTO_INCREMENT,
	name INT, 
	PRIMARY KEY(pid));

CREATE TABLE dog (
    did BIGINT NOT NULL AUTO_INCREMENT,
	name INT, 
	race varchar(255),
	PRIMARY KEY(did));

create table dog_friends (
    pid BIGINT NOT NULL,
    did BIGINT NOT NULL,
    UNIQUE(pid,did));

    select person.name, dog.name 
    from person 
        join dog_friends df on person.pid = df.pid join ( 
		select p.pid, count(*) noOfFriends 
		from person p 
		    join dog_friends df on p.pid = df.pid 
		having noOfFriends > 3
	) lotsOfFriends on person.pid = lotsOfFriends.pid join dog on df.did = dog.did 
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	_ = SetupExternal(t, boosttest.WithTestRecipe(recipe))
}

func TestMikesProductionSchema(t *testing.T) {
	const Recipe = `
CREATE TABLE mike (
	id int unsigned NOT NULL AUTO_INCREMENT,
	season varchar(255),
	PRIMARY KEY (id)
) ENGINE InnoDB,
  CHARSET utf8mb4,
  COLLATE utf8mb4_0900_ai_ci;

select /*vt+ VIEW=query */ id from mike where season = ?;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 0; i < 100; i += 10 {
		g.TestExecute(
			"insert into mike (season) values ('fall-%d'),('fall-%d'),('fall-%d'),('fall-%d'),('fall-%d'),('fall-%d'),('fall-%d'),('fall-%d'),('fall-%d'),('fall-%d')",
			i, i+1, i+2, i+3, i+4, i+5, i+6, i+7, i+8, i+9,
		)
	}

	boosttest.Settle()

	g.View("query").Lookup("fall-69").Expect([]sqltypes.Row{{sqltypes.NewUint32(70)}})
}

func TestCollationsInJoins(t *testing.T) {
	const Recipe = `
CREATE TABLE article (id bigint, title varchar(255) collate TEST_COLLATION, PRIMARY KEY(id));
CREATE TABLE vote (id bigint NOT NULL AUTO_INCREMENT, article_title varchar(255) collate TEST_COLLATION, user bigint, PRIMARY KEY(id));

SELECT /*vt+ VIEW=complexjoin PUBLIC */ article.id, article.title, votecount.votes AS votes
FROM article
    LEFT JOIN (SELECT vote.article_title, COUNT(vote.user) AS votes
               FROM vote GROUP BY vote.article_title) AS votecount
    ON (article.title = votecount.article_title) WHERE article.title = :article_title;

select /*vt+ VIEW=simplejoin PUBLIC */ article.id, article.title, vote.id, vote.article_title 
	FROM article LEFT JOIN vote ON (article.title = vote.article_title) WHERE article.title = :article_title;
`

	var Collations = []struct {
		Name          string
		CaseSensitive bool
	}{
		{
			Name:          "utf8mb4_0900_ai_ci",
			CaseSensitive: false,
		},
		{
			Name:          "utf8mb4_0900_bin",
			CaseSensitive: true,
		},
	}

	for _, coll := range Collations {
		t.Run(coll.Name, func(t *testing.T) {
			recipe := testrecipe.LoadSQL(t, strings.ReplaceAll(Recipe, "TEST_COLLATION", coll.Name))

			g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

			const articleCount = 10
			for i := 0; i < articleCount; i++ {
				g.TestExecute("INSERT INTO `article` (`id`, `title`) VALUES (%d, 'Article %d')", i+1, i+1)
			}

			const userCount = 10
			const votesCount = 100
			for i := 0; i < votesCount; i++ {
				g.TestExecute("INSERT INTO `vote` (`article_title`, `user`) values ('article %d', %d)", (i%articleCount)+1, rand.Intn(userCount)+1)
				g.TestExecute("INSERT INTO `vote` (`article_title`, `user`) values ('Article %d', %d)", (i%articleCount)+1, rand.Intn(userCount)+1)
			}

			time.Sleep(10 * time.Millisecond)

			expectedCount := votesCount / articleCount
			if !coll.CaseSensitive {
				expectedCount *= 2
			}

			join1 := g.View("complexjoin")

			join1.Lookup("Article 1").Expect([]sqltypes.Row{{sqltypes.NewInt64(1), sqltypes.NewVarChar("Article 1"), sqltypes.NewInt64(int64(expectedCount))}})
			join1.Lookup("Article 2").Expect([]sqltypes.Row{{sqltypes.NewInt64(2), sqltypes.NewVarChar("Article 2"), sqltypes.NewInt64(int64(expectedCount))}})

			if coll.CaseSensitive {
				join1.Lookup("ARTICLE 1").Expect([]sqltypes.Row{})
				join1.Lookup("ARTICLE 2").Expect([]sqltypes.Row{})
			} else {
				join1.Lookup("ARTICLE 1").Expect([]sqltypes.Row{{sqltypes.NewInt64(1), sqltypes.NewVarChar("Article 1"), sqltypes.NewInt64(int64(expectedCount))}})
				join1.Lookup("ARTICLE 2").Expect([]sqltypes.Row{{sqltypes.NewInt64(2), sqltypes.NewVarChar("Article 2"), sqltypes.NewInt64(int64(expectedCount))}})
			}

			join2 := g.View("simplejoin")
			join2.Lookup("Article 1").ExpectLen(expectedCount)

			require.Equal(t, 5, g.WorkerReads())
			require.Equal(t, articleCount+(2*votesCount), g.WorkerStats(worker.StatVStreamRows))
		})
	}
}

func TestOuterJoinWithLiteralComparison(t *testing.T) {
	const Recipe = `
CREATE TABLE article (id bigint, title varchar(255), PRIMARY KEY(id));
CREATE TABLE vote (id bigint NOT NULL AUTO_INCREMENT, article_title varchar(255), user bigint, PRIMARY KEY(id));

SELECT /*vt+ VIEW=outer2 PUBLIC */ COUNT(1) FROM article LEFT JOIN vote ON vote.article_title="ticket_price" AND article.id = vote.id;
SELECT /*vt+ VIEW=inner1 PUBLIC */ COUNT(1) FROM article JOIN vote ON vote.article_title="ticket_price" AND article.id = vote.id;
SELECT /*vt+ VIEW=inner2 PUBLIC */ COUNT(1) FROM article JOIN vote ON article.title="ticket_price" AND article.id = vote.id;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	_ = SetupExternal(t, boosttest.WithTestRecipe(recipe))
}

func TestRepositoriesWithIn(t *testing.T) {
	recipe := testrecipe.Load(t, "repositories")
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 0; i < 10; i++ {
		g.TestExecute(
			"insert into repositories (name, url, created_at, updated_at) values ('repo-%d', 'https://repo-%d.com', NOW(), NOW())", i, i,
		)
	}

	for i := 0; i < 10; i++ {
		g.TestExecute(
			"insert into tags (name, created_at, updated_at) values ('tag-%d', NOW(), NOW())", i,
		)
	}

	for i := 0; i < 10; i++ {
		g.TestExecute(
			"insert into repository_tags (tag_id, repository_id, created_at, updated_at) values (%d, %d, NOW(), NOW())", i, i,
		)
	}

	for i := 0; i < 10; i++ {
		g.TestExecute(
			"insert into stars (user_id, repository_id, created_at, updated_at) values (%d, %d, NOW(), NOW())", i, i,
		)
	}

	boosttest.Settle()

	g.View("query_with_in").LookupBvar([]any{"tag-7", "tag-8"}).Expect(
		[]sqltypes.Row{
			{sqltypes.NewInt64(1), sqltypes.NewInt64(8)},
			{sqltypes.NewInt64(1), sqltypes.NewInt64(9)},
		})
}

func TestUpqueryInPlan(t *testing.T) {
	const Recipe = `
	CREATE TABLE conv (pk BIGINT NOT NULL AUTO_INCREMENT,
	col_int INT, col_double DOUBLE, col_decimal DECIMAL, col_char VARCHAR(255),
	PRIMARY KEY(pk));

	SELECT /*vt+ VIEW=upquery PUBLIC */ conv.pk FROM conv WHERE conv.col_char IN ::x;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 3; i++ {
		g.TestExecute("INSERT INTO conv (col_int, col_double, col_decimal, col_char) VALUES (%d, %f, %f, 'string-%d')", i, float64(i), float64(i), i)
	}

	g.View("upquery").LookupBvar([]any{"string-1", "string-2"}).Expect([]sqltypes.Row{{sqltypes.NewInt64(1)}, {sqltypes.NewInt64(2)}})
}

func TestMultipleUpqueries(t *testing.T) {
	const Recipe = `
	CREATE TABLE tbl (pk BIGINT NOT NULL AUTO_INCREMENT, 
	a BIGINT, b BIGINT, c BIGINT,
	PRIMARY KEY(pk));

	SELECT /*vt+ VIEW=upquery0 PUBLIC */ tbl.pk FROM tbl WHERE tbl.a = :a and tbl.b = :b and tbl.c = :c;
	SELECT /*vt+ VIEW=upquery1 PUBLIC */ tbl.pk FROM tbl WHERE tbl.a IN ::a and tbl.b = :b;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 16; i++ {
		g.TestExecute("INSERT INTO tbl (a, b, c) VALUES (%d, %d, %d)", i, i%4, i*10)
	}

	upquery0 := g.View("upquery0")
	upquery0.Lookup(1, 1, 10).Expect([]sqltypes.Row{{sqltypes.NewInt64(1)}})

	upquery1 := g.View("upquery1")
	upquery1.LookupBvar([]any{4, 8, 12}, 0).Expect(
		[]sqltypes.Row{
			{sqltypes.NewInt64(4)},
			{sqltypes.NewInt64(8)},
			{sqltypes.NewInt64(12)},
		})
}

func TestTypeConversions(t *testing.T) {
	const Recipe = `
	CREATE TABLE conv (pk BIGINT NOT NULL AUTO_INCREMENT,
	col_int INT, col_double DOUBLE, col_decimal DECIMAL, col_char VARCHAR(255),
	PRIMARY KEY(pk));

	SELECT /*vt+ VIEW=conv0 PUBLIC */ conv.pk FROM conv WHERE conv.col_int = :x;
	SELECT /*vt+ VIEW=conv1 PUBLIC */ conv.pk FROM conv WHERE conv.col_double = :x;
	SELECT /*vt+ VIEW=conv2 PUBLIC */ conv.pk FROM conv WHERE conv.col_char = :x;
	SELECT /*vt+ VIEW=conv3 PUBLIC */ conv.pk FROM conv WHERE conv.col_char IN ::x;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 3; i++ {
		g.TestExecute("INSERT INTO conv (col_int, col_double, col_decimal, col_char) VALUES (%d, %f, %f, 'string-%d')", i, float64(i), float64(i), i)
	}

	conv0 := g.View("conv0")
	conv0.Lookup("1").Expect([]sqltypes.Row{{sqltypes.NewInt64(1)}})
	conv0.Lookup("1.0").Expect([]sqltypes.Row{{sqltypes.NewInt64(1)}})
	conv0.Lookup(1.0).Expect([]sqltypes.Row{{sqltypes.NewInt64(1)}})
	conv0.Lookup("1.5").Expect([]sqltypes.Row{})
	conv0.Lookup(1.5).Expect([]sqltypes.Row{})

	conv1 := g.View("conv1")
	conv1.Lookup("1").Expect([]sqltypes.Row{{sqltypes.NewInt64(1)}})
	conv1.Lookup("1.0").Expect([]sqltypes.Row{{sqltypes.NewInt64(1)}})
	conv1.Lookup(1.0).Expect([]sqltypes.Row{{sqltypes.NewInt64(1)}})
	conv1.Lookup("1.5").Expect([]sqltypes.Row{})
	conv1.Lookup(1.5).Expect([]sqltypes.Row{})

	conv2 := g.View("conv2")
	conv2.Lookup("string-1").Expect([]sqltypes.Row{{sqltypes.NewInt64(1)}})
	conv2.Lookup(1).ExpectError()
	conv2.Lookup(1.0).ExpectError()

	conv3 := g.View("conv3")
	conv3.LookupBvar([]any{"string-1", "string-2"}).ExpectLen(2)
	conv3.LookupBvar([]any{"string-1", 1}).ExpectError()
}

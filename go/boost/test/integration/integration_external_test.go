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
	querypb "vitess.io/vitess/go/vt/proto/query"
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

	g.View("v_partial").AssertLookup(nil, []sqltypes.Row{})

	g.View("v_partial").AssertLookup([]sqltypes.Value{sqltypes.NewInt64(1)}, []sqltypes.Row{{sqltypes.NewInt64(1)}})
	time.Sleep(500 * time.Millisecond)
	g.View("v_full").AssertLookup(nil, []sqltypes.Row{{sqltypes.NewDecimal("6")}})
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

	g.View("v_sum_b").AssertLookup([]sqltypes.Value{sqltypes.NewInt64(2)}, []sqltypes.Row{{sqltypes.NewDecimal("5")}})

	time.Sleep(50 * time.Millisecond)
	g.TestExecute("INSERT INTO num (a, b) VALUES (%d, %d)", 2, 3)
	g.TestExecute("INSERT INTO num (a, b) VALUES (%d, %d)", 3, 2)
	g.TestExecute("INSERT INTO num (a, b) VALUES (%d, %d)", 3, 3)

	time.Sleep(200 * time.Millisecond)
	g.View("v_sum_a").AssertLookup([]sqltypes.Value{sqltypes.NewInt64(2)}, []sqltypes.Row{{sqltypes.NewDecimal("6")}})

	time.Sleep(500 * time.Millisecond)
	g.View("v_sum_b").AssertLookup([]sqltypes.Value{sqltypes.NewInt64(2)}, []sqltypes.Row{{sqltypes.NewDecimal("8")}})
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

	g.View("op0").AssertLookup([]sqltypes.Value{sqltypes.NewInt64(1)}, []sqltypes.Row{
		{sqltypes.NewInt32(1), sqltypes.NewInt64(2), sqltypes.NewInt64(420)},
	})

	g.View("op0").AssertLookup([]sqltypes.Value{sqltypes.NewVarChar("1")}, []sqltypes.Row{
		{sqltypes.NewInt32(1), sqltypes.NewInt64(2), sqltypes.NewInt64(420)},
	})

	g.View("op1").AssertLookup(nil, []sqltypes.Row{
		{sqltypes.NewInt32(1), sqltypes.NewInt64(2), sqltypes.NewInt64(420)},
		{sqltypes.NewInt32(2), sqltypes.NewInt64(4), sqltypes.NewInt64(420)},
		{sqltypes.NewInt32(3), sqltypes.NewInt64(6), sqltypes.NewInt64(420)},
	})

	g.View("op3").AssertLookup([]sqltypes.Value{sqltypes.NewInt64(1)}, []sqltypes.Row{
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

	g.View("op0").AssertLookup([]sqltypes.Value{sqltypes.NewInt64(1)}, []sqltypes.Row{
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

	g.View("op0").AssertLookup([]sqltypes.Value{sqltypes.NewInt64(1)}, []sqltypes.Row{
		{sqltypes.NewInt32(1), sqltypes.NewInt64(2), sqltypes.NewInt64(420)},
	})
	g.View("op1").AssertLookup([]sqltypes.Value{sqltypes.NewInt64(10)}, []sqltypes.Row{
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

	g.View("op0").AssertLookup(nil, []sqltypes.Row{
		{sqltypes.NewInt64(3), sqltypes.NewInt64(4)},
	})
	g.View("op1").AssertLookup(nil, []sqltypes.Row{
		{sqltypes.NewInt32(100), sqltypes.NewInt64(1), sqltypes.NewInt64(2)},
		{sqltypes.NewInt32(200), sqltypes.NewInt64(1), sqltypes.NewInt64(1)},
		{sqltypes.NewInt32(300), sqltypes.NewInt64(1), sqltypes.NewInt64(1)},
	})
	g.View("op2").AssertLookup([]sqltypes.Value{sqltypes.NewInt64(2)}, []sqltypes.Row{
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

	g.View("op0").AssertLookup(nil, []sqltypes.Row{
		{sqltypes.NewInt64(3), sqltypes.NewInt64(4)},
	})
	g.View("op1").AssertLookup(nil, []sqltypes.Row{
		{sqltypes.NewInt32(100), sqltypes.NewInt64(1), sqltypes.NewInt64(2)},
		{sqltypes.NewInt32(200), sqltypes.NewInt64(1), sqltypes.NewInt64(1)},
		{sqltypes.NewInt32(300), sqltypes.NewInt64(1), sqltypes.NewInt64(1)},
	})
	g.View("op2").AssertLookup([]sqltypes.Value{sqltypes.NewInt64(2)}, []sqltypes.Row{
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

	identity := g.View("identity")
	res := identity.Lookup([]sqltypes.Value{sqltypes.NewInt64(1)})
	t.Logf("lookup identity: %v", res.Rows)

	g.View("summed_a").AssertLookup(nil, []sqltypes.Row{{sqltypes.NewDecimal("6")}})
	g.View("summed_g").AssertLookup(nil, []sqltypes.Row{{sqltypes.NewFloat64(6)}})

	g.View("max_a").AssertLookup(nil, []sqltypes.Row{{sqltypes.NewInt32(3)}})
	g.View("max_g").AssertLookup(nil, []sqltypes.Row{{sqltypes.NewFloat64(3)}})

	g.View("min_a").AssertLookup(nil, []sqltypes.Row{{sqltypes.NewInt32(1)}})
	g.View("min_g").AssertLookup(nil, []sqltypes.Row{{sqltypes.NewFloat64(1)}})
	g.View("multi").AssertLookup(nil, []sqltypes.Row{{sqltypes.NewDecimal("6"), sqltypes.NewFloat64(1)}})
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

	g.View("summed").AssertLookup(nil, []sqltypes.Row{{sqltypes.NewInt32(2), sqltypes.NewDecimal("14")}})
}

func TestTopK(t *testing.T) {
	const Recipe = `
CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT,
	a INT, 
	b INT,
	PRIMARY KEY(pk));

	SELECT /*vt+ VIEW=top PUBLIC */ a, b FROM num WHERE a = ? ORDER BY b LIMIT 3;
	SELECT /*vt+ VIEW=top_with_bogokey PUBLIC */ a, b FROM num ORDER BY b LIMIT 3;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 5; i++ {
		/*
			pk | a | b
			0  | 1 | 1
			1  | 2 | -1
			2  | 1 | 2
			3  | 2 | -4
			4  | 1 | 3
			3  | 2 | -9
		*/
		g.TestExecute("INSERT INTO num (a, b) VALUES (1, %d), (2, -%d)", i, i*i)
	}

	g.View("top").AssertLookup([]sqltypes.Value{sqltypes.NewInt32(2)},
		[]sqltypes.Row{
			{sqltypes.NewInt32(2), sqltypes.NewInt32(-9)},
			{sqltypes.NewInt32(2), sqltypes.NewInt32(-4)},
			{sqltypes.NewInt32(2), sqltypes.NewInt32(-1)},
		})

	g.View("top").AssertLookup([]sqltypes.Value{sqltypes.NewInt32(1)},
		[]sqltypes.Row{
			{sqltypes.NewInt32(1), sqltypes.NewInt32(3)},
			{sqltypes.NewInt32(1), sqltypes.NewInt32(4)},
			{sqltypes.NewInt32(1), sqltypes.NewInt32(5)},
		})

	g.View("top_with_bogokey").AssertLookup(nil,
		[]sqltypes.Row{
			{sqltypes.NewInt32(1), sqltypes.NewInt32(3)},
			{sqltypes.NewInt32(1), sqltypes.NewInt32(4)},
			{sqltypes.NewInt32(1), sqltypes.NewInt32(5)},
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
	g.View("distinct").AssertLookup(nil, expected)
	g.View("union_distinct").AssertLookup(nil, expected)

	g.View("distinct2").AssertLookup([]sqltypes.Value{sqltypes.NewInt32(4)},
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
	g.View("distinct").AssertLookup(nil, expected)
	g.View("union_distinct").AssertLookup(nil, expected)

	g.View("distinct2").AssertLookup([]sqltypes.Value{sqltypes.NewInt32(4)},
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

	g.View("caseaggr").AssertLookup(nil, []sqltypes.Row{
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
	cq.AssertLookup([]sqltypes.Value{id}, []sqltypes.Row{{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}})

	executor.TestExecute("INSERT INTO b VALUES (1, 4)")
	cq.AssertLookup([]sqltypes.Value{id}, []sqltypes.Row{
		{id, sqltypes.NewInt64(2)},
		{id, sqltypes.NewInt64(4)},
	})

	executor.TestExecute("DELETE FROM a WHERE c1 = 1")
	cq.AssertLookup([]sqltypes.Value{id}, []sqltypes.Row{
		{id, sqltypes.NewInt64(4)},
	})

	executor.TestExecute("UPDATE b SET c2 = 3 WHERE c1 = 1")
	cq.AssertLookup([]sqltypes.Value{id}, []sqltypes.Row{
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

	awvc.AssertLookup([]sqltypes.Value{sqltypes.NewInt64(1)}, []sqltypes.Row{{sqltypes.NewInt64(1), sqltypes.NewVarChar("Article 1"), sqltypes.NewInt64(votesCount / articleCount)}})
	awvc.AssertLookup([]sqltypes.Value{sqltypes.NewInt64(2)}, []sqltypes.Row{{sqltypes.NewInt64(2), sqltypes.NewVarChar("Article 2"), sqltypes.NewInt64(votesCount / articleCount)}})

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
	view.AssertLookup(nil, []sqltypes.Row{
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

	getter.AssertLookup(
		[]sqltypes.Value{sqltypes.NewVarChar("Volvo")},
		[]sqltypes.Row{{sqltypes.NewInt64(2)}},
	)

	require.Equal(t, 3, g.WorkerStats(worker.StatVStreamRows))
}

func TestDoubleShuffle(t *testing.T) {
	recipe := testrecipe.Load(t, "double-shuffle")
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	carPrice := g.View("CarPrice")
	g.TestExecute("INSERT INTO `Price` (`pid`, `price`) values (%d, %d)", 1, 100)
	g.TestExecute("INSERT INTO `Car` (`cid`, `pid`) values (%d, %d)", 1, 1)

	carPrice.AssertLookup(
		[]sqltypes.Value{sqltypes.NewInt64(1)},
		[]sqltypes.Row{{sqltypes.NewInt32(1), sqltypes.NewInt32(100)}},
	)

	carPrice.AssertLookup(
		[]sqltypes.Value{sqltypes.NewInt32(1)},
		[]sqltypes.Row{{sqltypes.NewInt32(1), sqltypes.NewInt32(100)}},
	)

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
	clientQ1.AssertLookup([]sqltypes.Value{sqltypes.NewInt64(1)}, []sqltypes.Row{{sqltypes.NewInt64(2)}})

	recipe.Queries = append(recipe.Queries, &vtboost.CachedQuery{
		PublicId: "c0ffe",
		Name:     "q2",
		Sql:      "SELECT a * 2 FROM num WHERE pk = :x",
		Keyspace: testrecipe.DefaultKeyspace,
	})

	g.ApplyRecipe(recipe)

	clientQ1.AssertLookup([]sqltypes.Value{sqltypes.NewInt64(1)}, []sqltypes.Row{{sqltypes.NewInt64(2)}})
	clientQ2 := g.View("q2")
	clientQ2.AssertLookup([]sqltypes.Value{sqltypes.NewInt64(1)}, []sqltypes.Row{{sqltypes.NewInt64(2)}})

	recipe.Queries = xslice.Filter(recipe.Queries, func(q *vtboost.CachedQuery) bool { return q.Name != "q1" })
	g.ApplyRecipe(recipe)

	time.Sleep(100 * time.Millisecond)

	require.Nil(t, g.FindView("q1"))
	clientQ2.AssertLookup([]sqltypes.Value{sqltypes.NewInt64(1)}, []sqltypes.Row{{sqltypes.NewInt64(2)}})
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
    PRIMARY KEY(pid,did));

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

	g.View("query").AssertLookup([]sqltypes.Value{sqltypes.NewVarChar("fall-69")}, []sqltypes.Row{{sqltypes.NewUint32(70)}})
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

			join1.AssertLookup([]sqltypes.Value{sqltypes.NewVarChar("Article 1")}, []sqltypes.Row{{sqltypes.NewInt64(1), sqltypes.NewVarChar("Article 1"), sqltypes.NewInt64(int64(expectedCount))}})
			join1.AssertLookup([]sqltypes.Value{sqltypes.NewVarChar("Article 2")}, []sqltypes.Row{{sqltypes.NewInt64(2), sqltypes.NewVarChar("Article 2"), sqltypes.NewInt64(int64(expectedCount))}})

			if coll.CaseSensitive {
				join1.AssertLookup([]sqltypes.Value{sqltypes.NewVarChar("ARTICLE 1")}, []sqltypes.Row{})
				join1.AssertLookup([]sqltypes.Value{sqltypes.NewVarChar("ARTICLE 2")}, []sqltypes.Row{})
			} else {
				join1.AssertLookup([]sqltypes.Value{sqltypes.NewVarChar("ARTICLE 1")}, []sqltypes.Row{{sqltypes.NewInt64(1), sqltypes.NewVarChar("Article 1"), sqltypes.NewInt64(int64(expectedCount))}})
				join1.AssertLookup([]sqltypes.Value{sqltypes.NewVarChar("ARTICLE 2")}, []sqltypes.Row{{sqltypes.NewInt64(2), sqltypes.NewVarChar("Article 2"), sqltypes.NewInt64(int64(expectedCount))}})
			}

			join2 := g.View("simplejoin")
			require.Len(t, join2.Lookup([]sqltypes.Value{sqltypes.NewVarChar("Article 1")}).Rows, expectedCount)

			require.Equal(t, 5, g.WorkerReads())
			require.Equal(t, articleCount+(2*votesCount), g.WorkerStats(worker.StatVStreamRows))
		})
	}
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

	g.View("query_with_in").AssertLookupBindVars([]*querypb.BindVariable{
		{
			Type: sqltypes.Tuple,
			Values: []*querypb.Value{
				{
					Type:  sqltypes.VarChar,
					Value: []byte("tag-7"),
				},
				{
					Type:  sqltypes.VarChar,
					Value: []byte("tag-8"),
				},
			},
		},
	}, []sqltypes.Row{
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

	g.View("upquery").AssertLookupBindVars([]*querypb.BindVariable{
		{
			Type: sqltypes.Tuple,
			Values: []*querypb.Value{
				{
					Type:  sqltypes.VarChar,
					Value: []byte("string-1"),
				},
				{
					Type:  sqltypes.VarChar,
					Value: []byte("string-2"),
				},
			},
		}}, []sqltypes.Row{
		{sqltypes.NewInt64(1)},
		{sqltypes.NewInt64(2)},
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
	conv0.AssertLookup([]sqltypes.Value{sqltypes.NewVarChar("1")}, []sqltypes.Row{{sqltypes.NewInt64(1)}})
	conv0.AssertLookup([]sqltypes.Value{sqltypes.NewVarChar("1.0")}, []sqltypes.Row{{sqltypes.NewInt64(1)}})
	conv0.AssertLookup([]sqltypes.Value{sqltypes.NewFloat64(1.0)}, []sqltypes.Row{{sqltypes.NewInt64(1)}})
	conv0.AssertLookup([]sqltypes.Value{sqltypes.NewVarChar("1.5")}, []sqltypes.Row{})
	conv0.AssertLookup([]sqltypes.Value{sqltypes.NewFloat64(1.5)}, []sqltypes.Row{})

	conv1 := g.View("conv1")
	conv1.AssertLookup([]sqltypes.Value{sqltypes.NewVarChar("1")}, []sqltypes.Row{{sqltypes.NewInt64(1)}})
	conv1.AssertLookup([]sqltypes.Value{sqltypes.NewVarChar("1.0")}, []sqltypes.Row{{sqltypes.NewInt64(1)}})
	conv1.AssertLookup([]sqltypes.Value{sqltypes.NewFloat64(1.0)}, []sqltypes.Row{{sqltypes.NewInt64(1)}})
	conv1.AssertLookup([]sqltypes.Value{sqltypes.NewVarChar("1.5")}, []sqltypes.Row{})
	conv1.AssertLookup([]sqltypes.Value{sqltypes.NewFloat64(1.5)}, []sqltypes.Row{})

	conv2 := g.View("conv2")
	conv2.AssertLookup([]sqltypes.Value{sqltypes.NewVarChar("string-1")}, []sqltypes.Row{{sqltypes.NewInt64(1)}})
	_, err := conv2.View.Lookup(context.Background(), []sqltypes.Value{sqltypes.NewInt64(1)}, true)
	require.Error(t, err)
	_, err = conv2.View.Lookup(context.Background(), []sqltypes.Value{sqltypes.NewFloat64(1.0)}, true)
	require.Error(t, err)

	conv3 := g.View("conv3")
	_, err = conv3.View.LookupByBindVar(context.Background(), []*querypb.BindVariable{
		{
			Type: sqltypes.Tuple,
			Values: []*querypb.Value{
				{
					Type:  sqltypes.VarChar,
					Value: []byte("string-1"),
				},
				{
					Type:  sqltypes.VarChar,
					Value: []byte("string-2"),
				},
			},
		},
	}, true)
	require.NoError(t, err)

	_, err = conv3.View.LookupByBindVar(context.Background(), []*querypb.BindVariable{
		{
			Type: sqltypes.Tuple,
			Values: []*querypb.Value{
				{
					Type:  sqltypes.VarChar,
					Value: []byte("string-1"),
				},
				{
					Type:  sqltypes.Int64,
					Value: []byte("1"),
				},
			},
		},
	}, true)
	require.Error(t, err)
}

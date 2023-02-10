package integration

import (
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/boost/common/xslice"
	"vitess.io/vitess/go/boost/server/worker"
	"vitess.io/vitess/go/boost/test/helpers/boosttest"
	"vitess.io/vitess/go/boost/test/helpers/boosttest/testrecipe"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/vtboost"
)

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

	awvc.Lookup(1).Expect(`[[INT64(1) VARCHAR("Article 1") INT64(10)]]`)
	awvc.Lookup(2).Expect(`[[INT64(2) VARCHAR("Article 2") INT64(10)]]`)

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
	view.Lookup().Expect(`[[VARCHAR("albumQ") INT32(4)] [VARCHAR("albumY") INT32(1)] [VARCHAR("albumY") INT32(2)] [VARCHAR("albumX") INT32(2)] [VARCHAR("albumX") INT32(3)] [VARCHAR("albumX") INT32(1)]]`)

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

func TestPokemon(t *testing.T) {
	recipe := testrecipe.Load(t, "pokemon")
	seed := func(g *boosttest.Cluster) {
		g.TestExecute(`insert into Pokemon (id, name, spriteUrl) values 
                                (1, 'Bulbasaur', 'bulbasaur.gif'), 
                                (2, 'Charizard', 'charizard.gif'), 
                                (3, 'Pikachu', 'pikachu.gif')`)
		g.TestExecute(`insert into Vote (id, createdAt, votedForId, votedAgainstId) values  
                                (0,'2023-01-02', 1, 2), 
                                (1,'2023-02-02', 3, 2), 
                                (2,'2023-02-02', 2, 1)`)
	}
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe), boosttest.WithSeed(seed))

	g.View("pokemon_votes").Lookup().
		Expect(
			`[[INT32(3) VARCHAR("Pikachu") VARCHAR("pikachu.gif") INT64(1) NULL]
					   [INT32(1) VARCHAR("Bulbasaur") VARCHAR("bulbasaur.gif") INT64(1) INT64(1)]
 					   [INT32(2) VARCHAR("Charizard") VARCHAR("charizard.gif") INT64(1) INT64(2)]]`)
}

func TestAlbumsRecipeWithInitialData(t *testing.T) {
	recipe := testrecipe.Load(t, "albums")

	seed := func(g *boosttest.Cluster) {
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
	}

	g := SetupExternal(t, boosttest.WithTestRecipe(recipe), boosttest.WithSeed(seed))

	view := g.View("album_friends")
	view.Lookup().Expect(`[[VARCHAR("albumQ") INT32(4)] [VARCHAR("albumY") INT32(1)] [VARCHAR("albumY") INT32(2)] [VARCHAR("albumX") INT32(2)] [VARCHAR("albumX") INT32(3)] [VARCHAR("albumX") INT32(1)]]`)
}

func TestLoadingExternalRecipes(t *testing.T) {
	var recipes = []struct {
		name      string
		supported bool
	}{
		{"lobsters-schema", false},
		{"tpc-h", false},
		{"tpc-w", false},
		{"connections", true},
	}
	for _, r := range recipes {
		t.Run(r.name, func(t *testing.T) {
			if !r.supported {
				t.Skipf("schema %q is not yet supported by the planner", r.name)
			}
			recipe := testrecipe.Load(t, r.name)
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

	getter.Lookup("Volvo").Expect(`[[INT64(2)]]`)

	require.Equal(t, 3, g.WorkerStats(worker.StatVStreamRows))
}

func TestDoubleShuffle(t *testing.T) {
	recipe := testrecipe.Load(t, "double-shuffle")
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	carPrice := g.View("CarPrice")
	g.TestExecute("INSERT INTO `Price` (`pid`, `price`) values (%d, %d)", 1, 100)
	g.TestExecute("INSERT INTO `Car` (`cid`, `pid`) values (%d, %d)", 1, 1)

	carPrice.Lookup(1).Expect(`[[INT32(1) INT32(100)]]`)
	carPrice.Lookup(int32(1)).Expect(`[[INT32(1) INT32(100)]]`)

	require.Equal(t, 2, g.WorkerStats(worker.StatVStreamRows))

	g.TestExecute("UPDATE `Price` SET `price` = %d WHERE `pid` = %d", 200, 1)
	time.Sleep(100 * time.Millisecond)

	carPrice.Lookup(1).Expect(`[[INT32(1) INT32(200)]]`)
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
		PublicId: "q1",
		Sql:      "SELECT a + 1 FROM num WHERE pk = :x",
		Keyspace: testrecipe.DefaultKeyspace,
	})

	g.ApplyRecipe(recipe)

	clientQ1 := g.View("q1")
	clientQ1.Lookup(1).Expect(`[[INT64(2)]]`)

	recipe.Queries = append(recipe.Queries, &vtboost.CachedQuery{
		PublicId: "q2",
		Sql:      "SELECT a * 2 FROM num WHERE pk = :x",
		Keyspace: testrecipe.DefaultKeyspace,
	})

	g.ApplyRecipe(recipe)

	clientQ1.Lookup(1).Expect(`[[INT64(2)]]`)

	clientQ2 := g.View("q2")
	clientQ2.Lookup(1).Expect(`[[INT64(2)]]`)

	recipe.Queries = xslice.Filter(recipe.Queries, func(q *vtboost.CachedQuery) bool { return q.PublicId != "q1" })
	g.ApplyRecipe(recipe)

	time.Sleep(100 * time.Millisecond)

	require.Nil(t, g.FindView("q1"))
	clientQ2.Lookup(1).Expect(`[[INT64(2)]]`)
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

	g.View("query_with_in").LookupBvar([]any{"tag-7", "tag-8"}).Expect(`[[INT64(1) INT64(8)] [INT64(1) INT64(9)]]`)
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
		select p.pid, count(*) as noOfFriends 
		from person p 
		    join dog_friends df on p.pid = df.pid
		group by p.pid
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

select id from mike where season = ?;
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

	g.View("q0").Lookup("fall-69").Expect(`[[UINT32(70)]]`)
}

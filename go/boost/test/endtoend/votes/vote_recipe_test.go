package votes

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	vtboostpb "vitess.io/vitess/go/vt/proto/vtboost"

	"vitess.io/vitess/go/boost/server/worker"
	"vitess.io/vitess/go/boost/test/helpers/booste2e"
	"vitess.io/vitess/go/sqltypes"
)

const selectQuery = `
	SELECT article.id, article.title, votecount.votes AS votes
		FROM article
		LEFT JOIN (SELECT vote.article_id, COUNT(vote.user) AS votes
				   FROM vote GROUP BY vote.article_id) AS votecount
		ON (article.id = votecount.article_id) WHERE article.id = 1;
`

const articleCount = 10
const userCount = 10
const votesCount = 100

func TestEndtoendVoteRecipeWithExternalBase(t *testing.T) {
	tt := booste2e.Setup(t, booste2e.WithRecipe("votes"))
	awvc := tt.BoostTestCluster.View("articlewithvotecount")

	var expectedResult = sqltypes.Row{sqltypes.NewInt64(1), sqltypes.NewVarChar("Article 1"), sqltypes.NewInt64(votesCount / articleCount)}

	for i := 0; i < articleCount; i++ {
		tt.ExecuteFetch("INSERT INTO `article` (`id`, `title`) VALUES (%d, 'Article %d')", i+1, i+1)
	}

	for i := 0; i < votesCount; i++ {
		tt.ExecuteFetch("INSERT INTO `vote` (`article_id`, `user`) values (%d, %d)", (i%articleCount)+1, rand.Intn(userCount)+1)
	}

	time.Sleep(1 * time.Second)

	awvc.Lookup(1).Expect(`[[INT64(1) VARCHAR("Article 1") INT64(10)]]`)
	awvc.Lookup(2).Expect(`[[INT64(2) VARCHAR("Article 2") INT64(10)]]`)

	tt.ExecuteFetch("SET @@boost_cached_queries = false")

	rs := tt.ExecuteFetch(selectQuery)
	require.Len(t, rs.Rows, 1)
	require.Equal(t, expectedResult, rs.Rows[0])
	tt.BoostTestCluster.AssertWorkerStats(2, worker.StatViewReads)

	tt.ExecuteFetch("SET @@boost_cached_queries = true")

	rs = tt.ExecuteFetch(selectQuery)
	require.Len(t, rs.Rows, 1)
	require.Equal(t, expectedResult, rs.Rows[0])
	tt.BoostTestCluster.AssertWorkerStats(3, worker.StatViewReads)
	tt.BoostTestCluster.AssertWorkerStats(articleCount+votesCount, worker.StatVStreamRows)
}

func TestEndtoendVoteRecipeRemoval(t *testing.T) {
	tt := booste2e.Setup(t, booste2e.WithRecipe("votes"))

	var expectedResult = sqltypes.Row{sqltypes.NewInt64(1), sqltypes.NewVarChar("Article 1"), sqltypes.NewInt64(votesCount / articleCount)}

	for i := 0; i < articleCount; i++ {
		tt.ExecuteFetch("INSERT INTO `article` (`id`, `title`) VALUES (%d, 'Article %d')", i+1, i+1)
	}

	for i := 0; i < votesCount; i++ {
		tt.ExecuteFetch("INSERT INTO `vote` (`article_id`, `user`) values (%d, %d)", (i%articleCount)+1, rand.Intn(userCount)+1)
	}

	time.Sleep(1 * time.Second)

	tt.ExecuteFetch("SET @@boost_cached_queries = true")

	rs := tt.ExecuteFetch(selectQuery)
	require.Len(t, rs.Rows, 1)
	require.Equal(t, expectedResult, rs.Rows[0])
	tt.BoostTestCluster.AssertWorkerStats(1, worker.StatViewReads)
	tt.BoostTestCluster.AssertWorkerStats(articleCount+votesCount, worker.StatVStreamRows)

	recipe, err := tt.BoostTopo.GetRecipe(context.Background(), &vtboostpb.GetRecipeRequest{})
	require.NoError(t, err)
	_, err = tt.BoostTopo.PutRecipe(context.Background(), &vtboostpb.PutRecipeRequest{
		Recipe: &vtboostpb.Recipe{
			Queries: nil,
			Version: recipe.Recipe.Version + 1,
		},
	})
	require.NoError(t, err)

	time.Sleep(5 * time.Second)

	rs = tt.ExecuteFetch(selectQuery)
	require.Len(t, rs.Rows, 1)
	require.Equal(t, expectedResult, rs.Rows[0])

	tt.BoostTestCluster.AssertWorkerStats(1, worker.StatViewReads)
	tt.BoostTestCluster.AssertWorkerStats(articleCount+votesCount, worker.StatVStreamRows)

	_, err = tt.BoostTopo.PutRecipe(context.Background(), &vtboostpb.PutRecipeRequest{
		Recipe: &vtboostpb.Recipe{
			Queries: recipe.Recipe.Queries,
			Version: recipe.Recipe.Version + 2,
		},
	})
	require.NoError(t, err)

	time.Sleep(5 * time.Second)

	rs = tt.ExecuteFetch(selectQuery)
	require.Len(t, rs.Rows, 1)
	require.Equal(t, expectedResult, rs.Rows[0])
	tt.BoostTestCluster.AssertWorkerStats(2, worker.StatViewReads)
	tt.BoostTestCluster.AssertWorkerStats(articleCount+votesCount, worker.StatVStreamRows)
}

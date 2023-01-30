package integration

import (
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/boost/server/worker"
	"vitess.io/vitess/go/boost/test/helpers/boosttest"
	"vitess.io/vitess/go/boost/test/helpers/boosttest/testrecipe"
	"vitess.io/vitess/go/sqltypes"
)

func TestCollationsInJoins(t *testing.T) {
	const Recipe = `
CREATE TABLE article (id bigint, title varchar(255) collate TEST_COLLATION, PRIMARY KEY(id));
CREATE TABLE vote (id bigint NOT NULL AUTO_INCREMENT, article_title varchar(255) collate TEST_COLLATION, user bigint, PRIMARY KEY(id));

SELECT /*vt+ VIEW=complexjoin */ article.id, article.title, votecount.votes AS votes
FROM article
    LEFT JOIN (SELECT vote.article_title, COUNT(vote.user) AS votes
               FROM vote GROUP BY vote.article_title) AS votecount
    ON (article.title = votecount.article_title) WHERE article.title = :article_title;

select /*vt+ VIEW=simplejoin */ article.id, article.title, vote.id, vote.article_title 
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

			join1.Lookup("Article 1").ExpectRow([]sqltypes.Row{{sqltypes.NewInt64(1), sqltypes.NewVarChar("Article 1"), sqltypes.NewInt64(int64(expectedCount))}})
			join1.Lookup("Article 2").ExpectRow([]sqltypes.Row{{sqltypes.NewInt64(2), sqltypes.NewVarChar("Article 2"), sqltypes.NewInt64(int64(expectedCount))}})

			if coll.CaseSensitive {
				join1.Lookup("ARTICLE 1").ExpectRow([]sqltypes.Row{})
				join1.Lookup("ARTICLE 2").ExpectRow([]sqltypes.Row{})
			} else {
				join1.Lookup("ARTICLE 1").ExpectRow([]sqltypes.Row{{sqltypes.NewInt64(1), sqltypes.NewVarChar("Article 1"), sqltypes.NewInt64(int64(expectedCount))}})
				join1.Lookup("ARTICLE 2").ExpectRow([]sqltypes.Row{{sqltypes.NewInt64(2), sqltypes.NewVarChar("Article 2"), sqltypes.NewInt64(int64(expectedCount))}})
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
CREATE TABLE article (id bigint NOT NULL AUTO_INCREMENT, foo bigint, title varchar(255), PRIMARY KEY(id));
CREATE TABLE vote (id bigint NOT NULL AUTO_INCREMENT, foo bigint, article_title varchar(255), user bigint, PRIMARY KEY(id));

SELECT article.foo, vote.foo FROM article LEFT JOIN vote ON article.title="ticket_price" AND article.foo = vote.foo;
SELECT article.foo, vote.foo FROM article LEFT JOIN vote ON vote.article_title="ticket_price" AND article.foo = vote.foo;
SELECT article.foo, vote.foo FROM article JOIN vote ON vote.article_title="ticket_price" AND article.foo = vote.foo;
`

	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	g.TestExecute("INSERT INTO `article` (foo, title) VALUES (1, 'first'), (2, 'ticket_price')")
	g.TestExecute("INSERT INTO `vote` (foo, article_title) VALUES (1, 'ticket_price'), (2, 'apa')")

	g.View("q0").Lookup().Expect(`[[INT64(1) NULL] [INT64(2) INT64(2)]]`)
	g.View("q1").Lookup().Expect(`[[INT64(1) INT64(1)] [INT64(2) NULL]]`)
	g.View("q2").Lookup().Expect(`[[INT64(1) INT64(1)]]`)
}

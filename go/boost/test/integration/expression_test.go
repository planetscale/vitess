package integration

import (
	"testing"

	"vitess.io/vitess/go/boost/server/controller/config"
	"vitess.io/vitess/go/boost/test/helpers/boosttest"
	"vitess.io/vitess/go/boost/test/helpers/boosttest/testrecipe"
)

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

func TestDatetimeFormatFunction(t *testing.T) {
	const Recipe = `
	CREATE TABLE likes (
	user_id bigint NOT NULL,
	video_id bigint NOT NULL,
	deleted_at datetime,
	created_at datetime,
	PRIMARY KEY (user_id, video_id)
) ENGINE InnoDB,
  CHARSET utf8mb4,
  COLLATE utf8mb4_0900_ai_ci;

select count(*) from likes where date_format(created_at, '%Y%m%d') = ?;
`

	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))
	g.TestExecute(`INSERT INTO likes (user_id, video_id, created_at) VALUES
		 (1, 1, '2019-01-01 00:00:00'), (2, 2, '2019-01-02 00:00:00'),
         (2, 3, '2019-01-02 00:00:00'), (3, 3, '2019-01-03 00:00:00')`)
	g.View("q0").Lookup("20190102").Expect(`[[INT64(2)]]`)

	g.TestExecute(`INSERT INTO likes (user_id, video_id, created_at) VALUES
		 (1, 2, '2019-01-01 10:00:00'), (3, 2, '2019-01-02 12:00:00')`)

	g.View("q0").Lookup("20190102").Expect(`[[INT64(3)]]`)
}

func TestMultiColumnDependencyWithMidflow(t *testing.T) {
	const Recipe = `
	CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT, b INT, a INT, PRIMARY KEY(pk));
SELECT count(*) FROM num WHERE a + b = ?;`

	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	g.TestExecute("INSERT INTO num (a, b) VALUES (1, 2), (1, 3), (1, 4), (2, 3), (3, 2), (3, 4)")
	g.View("q0").Lookup(5).Expect(`[[INT64(3)]]`)
}

func TestMultiColumnDependencyWithoutMidflow(t *testing.T) {
	const Recipe = `
	CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT, b INT, a INT, PRIMARY KEY(pk));
SELECT count(*) FROM num WHERE a + b = ?;`

	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe), boosttest.WithCustomBoostConfig(func(cfg *config.Config) {
		cfg.Materialization.UpqueryMode = config.UpqueryGenerationMode_NO_MIDFLOW_UPQUERIES
	}))

	g.TestExecute("INSERT INTO num (a, b) VALUES (1, 2), (1, 3), (1, 4), (2, 3), (3, 2), (3, 4)")
	g.View("q0").Lookup(5).Expect(`[[INT64(3)]]`)
}

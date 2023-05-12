package integration

import (
	"testing"

	"vitess.io/vitess/go/boost/test/helpers/boosttest"
	"vitess.io/vitess/go/boost/test/helpers/boosttest/testrecipe"
)

func TestUnionSingle(t *testing.T) {
	const Recipe = `
CREATE TABLE sbtest1 (
	id int NOT NULL AUTO_INCREMENT,
	k int NOT NULL DEFAULT '0',
	c char(120) NOT NULL DEFAULT '',
	pad char(60) NOT NULL DEFAULT '',
	PRIMARY KEY (id)
) ENGINE InnoDB,
  CHARSET utf8mb4,
  COLLATE utf8mb4_0900_ai_ci;

CREATE TABLE sbtest2 (
	id int NOT NULL AUTO_INCREMENT,
	k int NOT NULL DEFAULT '0',
	c char(120) NOT NULL DEFAULT '',
	pad char(60) NOT NULL DEFAULT '',
	PRIMARY KEY (id)
) ENGINE InnoDB,
  CHARSET utf8mb4,
  COLLATE utf8mb4_0900_ai_ci;

select sum(k) from sbtest1
union
select sum(k) from sbtest2
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 1; i <= 3; i++ {
		g.TestExecute("INSERT INTO sbtest1 (k) VALUES (%d)", i)
	}

	for i := 2; i <= 4; i++ {
		g.TestExecute("INSERT INTO sbtest2 (k) VALUES (%d)", i)
	}

	g.View("q0").Lookup().Expect(`[[DECIMAL(6)] [DECIMAL(9)]]`)
}

func TestUnionMultiple(t *testing.T) {
	const Recipe = `
CREATE TABLE sbtest1 (
	id int NOT NULL AUTO_INCREMENT,
	k int NOT NULL DEFAULT '0',
	c char(120) NOT NULL DEFAULT '',
	pad char(60) NOT NULL DEFAULT '',
	PRIMARY KEY (id)
) ENGINE InnoDB,
  CHARSET utf8mb4,
  COLLATE utf8mb4_0900_ai_ci;

CREATE TABLE sbtest2 (
	id int NOT NULL AUTO_INCREMENT,
	k int NOT NULL DEFAULT '0',
	c char(120) NOT NULL DEFAULT '',
	pad char(60) NOT NULL DEFAULT '',
	PRIMARY KEY (id)
) ENGINE InnoDB,
  CHARSET utf8mb4,
  COLLATE utf8mb4_0900_ai_ci;

CREATE TABLE sbtest3 (
	id int NOT NULL AUTO_INCREMENT,
	k int NOT NULL DEFAULT '0',
	c char(120) NOT NULL DEFAULT '',
	pad char(60) NOT NULL DEFAULT '',
	PRIMARY KEY (id)
) ENGINE InnoDB,
  CHARSET utf8mb4,
  COLLATE utf8mb4_0900_ai_ci;

CREATE TABLE sbtest4 (
	id int NOT NULL AUTO_INCREMENT,
	k int NOT NULL DEFAULT '0',
	c char(120) NOT NULL DEFAULT '',
	pad char(60) NOT NULL DEFAULT '',
	PRIMARY KEY (id)
) ENGINE InnoDB,
  CHARSET utf8mb4,
  COLLATE utf8mb4_0900_ai_ci;

select sum(k) from sbtest1
union distinct
select sum(k) from sbtest2
union distinct
select sum(k) from sbtest3
union distinct
select sum(k) from sbtest4;

select sum(k) from sbtest1
union all
select sum(k) from sbtest2
union all
select sum(k) from sbtest3
union all
select sum(k) from sbtest4;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	g.TestExecute("INSERT INTO sbtest1 (k) VALUES (1), (2), (3)")
	g.TestExecute("INSERT INTO sbtest2 (k) VALUES (2), (3), (4)")

	g.View("q0").Lookup().Expect(`[[DECIMAL(6)] [DECIMAL(9)] [NULL]]`)
	g.View("q1").Lookup().Expect(`[[DECIMAL(6)] [DECIMAL(9)] [NULL] [NULL]]`)
}

func TestUnionPlannerCrashes(t *testing.T) {
	const Recipe = `
CREATE TABLE a (
	c1 bigint NOT NULL,
	c2 bigint NOT NULL,
	PRIMARY KEY (c1)
);

CREATE TABLE b (
	c1 bigint NOT NULL,
	c2 bigint NOT NULL,
	PRIMARY KEY (c1)
);

SELECT * FROM (SELECT c1, c2 FROM a UNION SELECT c1, c2 FROM b) AS u WHERE u.c1 = :a;
SELECT * FROM (SELECT c1, c2 FROM a UNION SELECT c1, c2 FROM b) AS u ORDER BY u.c1;
SELECT * FROM (SELECT c1, c2 FROM a UNION SELECT c2, c1 FROM b) AS u ORDER BY u.c1;
`

	recipe := testrecipe.LoadSQL(t, Recipe)
	_ = SetupExternal(t, boosttest.WithTestRecipe(recipe))
}

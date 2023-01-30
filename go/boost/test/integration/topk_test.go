package integration

import (
	"testing"

	"vitess.io/vitess/go/boost/test/helpers/boosttest"
	"vitess.io/vitess/go/boost/test/helpers/boosttest/testrecipe"
)

func TestTopK(t *testing.T) {
	const Recipe = `
CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT,
	a INT, 
	b INT,
	PRIMARY KEY(pk));

	SELECT /*vt+ VIEW=top */ a, b FROM num WHERE a = ? ORDER BY b LIMIT 3;
	SELECT /*vt+ VIEW=top_with_bogokey */ a, b FROM num ORDER BY b LIMIT 3;
	SELECT /*vt+ VIEW=top_without_key */ pk FROM num WHERE a = ? ORDER BY b LIMIT 3;
	SELECT /*vt+ VIEW=top_multi */ pk FROM num WHERE a IN ::a ORDER BY b LIMIT 4;
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

	g.View("top").Lookup(int32(2)).ExpectSorted("[[INT32(2) INT32(-25)] [INT32(2) INT32(-16)] [INT32(2) INT32(-9)]]")
	g.View("top").Lookup(int32(1)).ExpectSorted("[[INT32(1) INT32(1)] [INT32(1) INT32(2)] [INT32(1) INT32(3)]]")
	g.View("top_with_bogokey").Lookup().ExpectSorted("[[INT32(2) INT32(-25)] [INT32(2) INT32(-16)] [INT32(2) INT32(-9)]]")
	g.View("top_without_key").Lookup(int32(2)).ExpectSorted("[[INT64(10)] [INT64(8)] [INT64(6)]]")
	g.View("top_multi").LookupBvar([]any{1, 2}).ExpectSorted("[[INT64(10)] [INT64(8)] [INT64(6)] [INT64(4)]]")
}

func TestTopKEvictions(t *testing.T) {
	const Recipe = `
	CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT, a INT, b INT, PRIMARY KEY(pk));
	SELECT a, b FROM num WHERE a = ? ORDER BY b LIMIT 3;
`
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe))

	for i := 0; i <= 32; i++ {
		g.TestExecute("INSERT INTO num (a, b) VALUES (%d, %d)", i%4, i)
	}

	g.View("q0").Lookup(0).ExpectSorted("[[INT32(0) INT32(0)] [INT32(0) INT32(4)] [INT32(0) INT32(8)]]")

	for i := 0; i <= 16; i++ {
		g.TestExecute("DELETE FROM num WHERE b IN (0, 4, 8)")
	}

	g.View("q0").Lookup(0).ExpectSorted("[[INT32(0) INT32(12)] [INT32(0) INT32(16)] [INT32(0) INT32(20)]]")
}

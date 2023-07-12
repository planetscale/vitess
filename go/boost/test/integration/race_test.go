package integration

import (
	"fmt"
	"testing"
	"time"

	"vitess.io/vitess/go/boost/server/controller/config"
	"vitess.io/vitess/go/boost/test/helpers/boosttest"
	"vitess.io/vitess/go/boost/test/helpers/boosttest/testexecutor"
	"vitess.io/vitess/go/boost/test/helpers/boosttest/testrecipe"
)

func TestMaterializationWithIgnoredPackets(t *testing.T) {
	const Recipe = `
CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT, a BIGINT, PRIMARY KEY(pk));
SELECT /*vt+ VIEW=v_partial */ num.a FROM num WHERE num.pk = :primary;
SELECT /*vt+ VIEW=v_full */ SUM(num.a) FROM num;
`

	modes := []config.UpqueryGenerationMode{
		config.UpqueryGenerationMode_FULL_MIDFLOW_UPQUERIES,
		config.UpqueryGenerationMode_NO_READER_MIDFLOW_UPQUERIES,
		config.UpqueryGenerationMode_NO_MIDFLOW_UPQUERIES,
	}
	for _, midflow := range modes {
		t.Run(fmt.Sprintf("midflow=%v", midflow), func(t *testing.T) {
			recipe := testrecipe.LoadSQL(t, Recipe)
			g := SetupExternal(t,
				boosttest.WithTestRecipe(recipe),
				boosttest.WithFakeExecutorOptions(func(options *testexecutor.Options) {
					options.VStreamRowLatency = 100 * time.Millisecond
				}),
				boosttest.WithCustomBoostConfig(func(cfg *config.Config) {
					cfg.Materialization.UpqueryMode = midflow
				}),
			)

			for i := 2; i <= 4; i++ {
				g.TestExecute("INSERT INTO num (a) VALUES (%d)", i)
			}

			g.View("v_partial").Lookup().Expect(`[]`)

			g.View("v_partial").Lookup(1).Expect(`[[INT64(2)]]`)
			time.Sleep(500 * time.Millisecond)
			g.View("v_full").Lookup().Expect(`[[DECIMAL(9)]]`)
			g.View("v_partial").Lookup(1).Expect(`[[INT64(2)]]`)
		})
	}
}

func TestMaterializationWithIgnoredPacketsAndMultiplePartial(t *testing.T) {
	const Recipe = `
CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT, a BIGINT, b BIGINT, PRIMARY KEY(pk));
SELECT /*vt+ VIEW=v_sum_b */ SUM(num.b) FROM num WHERE num.a = :a;
`
	executor := func(options *testexecutor.Options) {
		options.VStreamRowLatency = 100 * time.Millisecond
	}
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe), boosttest.WithFakeExecutorOptions(executor))

	g.TestExecute("INSERT INTO num (a, b) VALUES (%d, %d)", 1, 2)
	g.TestExecute("INSERT INTO num (a, b) VALUES (%d, %d)", 1, 3)
	g.TestExecute("INSERT INTO num (a, b) VALUES (%d, %d)", 2, 2)
	g.TestExecute("INSERT INTO num (a, b) VALUES (%d, %d)", 2, 3)

	g.View("v_sum_b").Lookup(2).Expect(`[[DECIMAL(5)]]`)

	time.Sleep(50 * time.Millisecond)
	g.TestExecute("INSERT INTO num (a, b) VALUES (%d, %d)", 2, 3)
	g.TestExecute("INSERT INTO num (a, b) VALUES (%d, %d)", 3, 2)
	g.TestExecute("INSERT INTO num (a, b) VALUES (%d, %d)", 3, 3)

	time.Sleep(500 * time.Millisecond)
	g.View("v_sum_b").Lookup(2).Expect(`[[DECIMAL(8)]]`)
	g.View("v_sum_b").Lookup(420).Expect(`[[NULL]]`)
}

func TestRaceInFullMaterializationMissingVStreamEvents(t *testing.T) {
	const Recipe = `
CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT, a BIGINT, b BIGINT, PRIMARY KEY(pk));
SELECT SUM(num.a) FROM num;
`
	executor := func(options *testexecutor.Options) {
		options.VStreamStartLatency = func() time.Duration { return 100 * time.Millisecond }
	}
	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe), boosttest.WithFakeExecutorOptions(executor))

	for i := 0; i < 4; i++ {
		g.TestExecute("INSERT INTO num (a, b) VALUES (%d, %d)", i, 2)
	}

	time.Sleep(500 * time.Millisecond)
	g.View("q0").Lookup().Expect(`[[DECIMAL(6)]]`)
}

func TestRaceInFullMaterializationDuplicatedVStreamEvents(t *testing.T) {
	const Recipe = `
CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT, a BIGINT, b BIGINT, PRIMARY KEY(pk));
SELECT SUM(num.a) FROM num;
`
	executor := func(options *testexecutor.Options) {
		options.QueryLatency = 200 * time.Millisecond
	}

	prefill := func(g *boosttest.Cluster) {
		go func() {
			time.Sleep(100 * time.Millisecond)
			for i := 1; i <= 4; i++ {
				g.TestExecute("INSERT INTO num (a, b) VALUES (%d, %d)", i, 2)
			}
		}()
	}

	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe), boosttest.WithFakeExecutorOptions(executor), boosttest.WithSeed(prefill))
	time.Sleep(500 * time.Millisecond)
	g.View("q0").Lookup().Expect(`[[DECIMAL(10)]]`)
}

func TestSlowVStreamStart(t *testing.T) {
	const Recipe = `
CREATE TABLE num (pk BIGINT NOT NULL AUTO_INCREMENT, a BIGINT, b BIGINT, PRIMARY KEY(pk));
SELECT SUM(num.a) FROM num;
`
	executor := func(options *testexecutor.Options) {
		first := true
		options.VStreamStartLatency = func() time.Duration {
			if first {
				first = false
				return 200 * time.Millisecond
			}
			return 0 * time.Millisecond
		}
	}

	recipe := testrecipe.LoadSQL(t, Recipe)
	g := SetupExternal(t, boosttest.WithTestRecipe(recipe),
		boosttest.WithFakeExecutorOptions(executor),
		boosttest.WithCustomBoostConfig(func(cfg *config.Config) {
			cfg.VstreamStartTimeout = 100 * time.Millisecond
		}),
	)
	g.View("q0").Lookup().Expect(`[[NULL]]`)
}

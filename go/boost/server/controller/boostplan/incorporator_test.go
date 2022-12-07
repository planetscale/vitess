package boostplan_test

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/boost/server/controller"

	"vitess.io/vitess/go/boost/server/controller/boostplan"
	"vitess.io/vitess/go/boost/server/controller/boostplan/integration"
)

var goldenSave = flag.Bool("save-golden", false, "save golden tests with updated values")

func TestGoldenIncorporatorCasesFromNoria(t *testing.T) {
	golden := integration.LoadGoldenTest(t, "integration/testdata/noria.json")

	t.Run("ItParses", func(t *testing.T) {
		mig := controller.NewDummyMigration()
		ti := boostplan.NewTestIncorporator(golden.SchemaInformation, mig)

		err := ti.AddQuery("main", "SELECT users.id from users;")
		require.NoError(t, err)
		err = mig.Commit()
		require.NoError(t, err)
	})

	t.Run("GoldenQueries", func(t *testing.T) {
		for _, query := range golden.Queries {
			mig := controller.NewDummyMigration()
			ti := boostplan.NewTestIncorporator(golden.SchemaInformation, mig)

			t.Logf("%s", query.SQL)

			err := ti.AddQuery(query.Keyspace, query.SQL)
			if *goldenSave {
				if err != nil {
					query.Error = err.Error()
				} else {
					query.Error = ""
				}
				continue
			}

			if query.Error != "" {
				if err == nil {
					t.Errorf("expected %q to fail incorporation (update golden tests?)", query.SQL)
				}
				t.Logf("\tSKIPPING: %s", query.Error)
				continue
			}
			require.NoError(t, err)
			err = mig.Commit()
			require.NoError(t, err)
		}
	})

	if *goldenSave {
		golden.Save(t)
	}
}

func TestUnsupportedQueries(t *testing.T) {
	unsupported := integration.LoadGoldenTest(t, "integration/testdata/unsupported.json")

	t.Run("handles unsupported queries", func(t *testing.T) {

		for _, query := range unsupported.Queries {
			mig := controller.NewDummyMigration()
			ti := boostplan.NewTestIncorporator(unsupported.SchemaInformation, mig)
			t.Logf("%s", query.SQL)

			err := ti.AddQuery(query.Keyspace, query.SQL)
			if err == nil {
				err = mig.Commit()
			}
			if *goldenSave {
				if err != nil {
					query.Error = err.Error()
				} else {
					query.Error = ""
				}
				continue
			}

			require.Errorf(t, err, "expected %q to fail incorporation (update golden tests?)", query.SQL)
			assert.EqualError(t, err, query.Error)
		}
		if *goldenSave {
			unsupported.Save(t)
		}
	})
}

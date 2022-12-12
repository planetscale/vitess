package boostplan

import (
	"github.com/google/uuid"

	"vitess.io/vitess/go/boost/server/controller/boostplan/operators"
	"vitess.io/vitess/go/vt/sqlparser"
)

// TestIncorporator is a no-op query incorporator that allows testing whether a given query
// can be properly planned by the Boost planner.
// It is NOT safe for concurrent use.
type TestIncorporator struct {
	inc    *Incorporator
	schema *SchemaInformation
	mig    Migration
}

// NewTestIncorporator creates a new test incorporator. The given schemaInfo must contain
// all the schema information for any base tables that are referenced by the queries that
// will be planned by this incorporator. It is usually safest to simply load all the
// schemas for all the keyspaces current available in the Vitess cluster.
func NewTestIncorporator(schemaInfo *SchemaInformation, mig Migration) *TestIncorporator {
	return &TestIncorporator{
		inc:    NewIncorporator(),
		schema: schemaInfo,
		mig:    mig,
	}
}

func (ti *TestIncorporator) add(keyspace string, stmt sqlparser.Statement) (*operators.TableReport, error) {
	qfp, err := ti.inc.AddParsedQuery(keyspace, stmt, uuid.NewString(), ti.mig, ti.schema)
	if err != nil {
		return nil, err
	}
	return qfp.GetTableReport(), nil
}

// AddQuery attempt to plan the given sql query, returning a detailed error if the
// planning was unsuccessful.
func (ti *TestIncorporator) AddQuery(keyspace, sql string) error {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return err
	}
	_, err = ti.add(keyspace, stmt)
	return err
}

// AddParsedQuery attempt to plan the given previously-parsed statement, returning a detailed
// error if the planning was unsuccessful.
func (ti *TestIncorporator) AddParsedQuery(keyspace string, stmt sqlparser.Statement) (*operators.TableReport, error) {
	return ti.add(keyspace, sqlparser.CloneStatement(stmt))
}

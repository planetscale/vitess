package boostplan

import (
	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/dataflow/flownode"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/server/controller/boostplan/operators"
	"vitess.io/vitess/go/vt/sqlparser"
)

type dummyMigration struct {
	counter graph.NodeIdx
	nodes   map[graph.NodeIdx]flownode.NodeImpl
}

var _ Migration = (*dummyMigration)(nil)

func (d *dummyMigration) next() graph.NodeIdx {
	n := d.counter
	d.counter++
	return n
}

func (d *dummyMigration) AddIngredient(_ string, _ []string, impl flownode.NodeImpl) graph.NodeIdx {
	n := d.next()
	d.nodes[n] = impl
	return n
}

func (d *dummyMigration) AddBase(_ string, _ []string, b flownode.AnyBase) graph.NodeIdx {
	n := d.next()
	d.nodes[n] = b
	return n
}

func (d *dummyMigration) Maintain(string, graph.NodeIdx, []int, []boostpb.ViewParameter, int) {
	// nop
}

func (d *dummyMigration) MaintainAnonymous(n graph.NodeIdx, key []int) {
	// nop
}

// TestIncorporator is a no-op query incorporator that allows testing whether a given query
// can be properly planned by the Boost planner.
// It is NOT safe for concurrent use.
type TestIncorporator struct {
	inc    *Incorporator
	schema *SchemaInformation
	mig    dummyMigration
}

// NewTestIncorporator creates a new test incorporator. The given schemaInfo must contain
// all the schema information for any base tables that are referenced by the queries that
// will be planned by this incorporator. It is usually safest to simply load all the
// schemas for all the keyspaces current available in the Vitess cluster.
func NewTestIncorporator(schemaInfo *SchemaInformation) *TestIncorporator {
	return &TestIncorporator{
		inc:    NewIncorporator(),
		schema: schemaInfo,
		mig: dummyMigration{
			nodes: map[graph.NodeIdx]flownode.NodeImpl{},
		},
	}
}

func (ti *TestIncorporator) add(keyspace string, stmt sqlparser.Statement) (*operators.TableReport, error) {
	qfp, err := ti.inc.AddParsedQuery(keyspace, stmt, "", true, &ti.mig, ti.schema)
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

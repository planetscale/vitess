package flownode

import (
	"vitess.io/vitess/go/boost/dataflow/flownode/flownodepb"
	"vitess.io/vitess/go/boost/sql"
)

type Table struct {
	primaryKey []int
	schema     []sql.Type
	keyspace   string
	name       string
}

func (table *Table) dataflow() {}

var _ NodeImpl = (*Table)(nil)

func (table *Table) PrimaryKey() []int {
	return table.primaryKey
}

func (table *Table) Schema() []sql.Type {
	return table.schema
}

func (table *Table) Keyspace() string {
	return table.keyspace
}

func (table *Table) Name() string {
	return table.name
}

func NewTable(keyspace, name string, primaryKey []int, schema []sql.Type) *Table {
	if primaryKey == nil {
		panic("NewTable without primary key")
	}
	if schema == nil {
		panic("NewTable without schema")
	}
	return &Table{
		primaryKey: primaryKey,
		schema:     schema,
		keyspace:   keyspace,
		name:       name,
	}
}

func (table *Table) ToProto() *flownodepb.Node_Table {
	ptable := &flownodepb.Node_Table{
		PrimaryKey: table.primaryKey,
		Keyspace:   table.keyspace,
		Name:       table.name,
		Schema:     table.schema,
	}
	return ptable
}

func NewTableFromProto(tbl *flownodepb.Node_Table) *Table {
	return NewTable(tbl.Keyspace, tbl.Name, tbl.PrimaryKey, tbl.Schema)
}

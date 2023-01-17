package boostplan

import (
	"context"
	"fmt"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type DDLSchemaSource interface {
	GetSchema(ctx context.Context, keyspace string) (*tabletmanagerdata.SchemaDefinition, error)
}

type DDLSchema struct {
	Source DDLSchemaSource
	Specs  map[string]map[string]*sqlparser.TableSpec
}

func NewDDLSchema(src DDLSchemaSource) *DDLSchema {
	return &DDLSchema{
		Source: src,
		Specs:  make(map[string]map[string]*sqlparser.TableSpec),
	}
}

func (ddl *DDLSchema) LoadTableSpec(keyspace, table string) (*sqlparser.TableSpec, error) {
	var schema map[string]*sqlparser.TableSpec
	var ok bool

	if schema, ok = ddl.Specs[keyspace]; !ok {
		if ddl.Source == nil {
			return nil, &UnknownTableError{Keyspace: keyspace, Table: table}
		}
		definition, err := ddl.Source.GetSchema(context.Background(), keyspace)
		if err != nil {
			return nil, err
		}

		schema = make(map[string]*sqlparser.TableSpec)
		for _, td := range definition.TableDefinitions {
			stmt, err := sqlparser.ParseStrictDDL(td.Schema)
			if err != nil {
				return nil, &SyntaxError{Query: td.Schema, Err: err}
			}
			if create, ok := stmt.(*sqlparser.CreateTable); ok {
				schema[create.Table.Name.String()] = create.TableSpec
			}
		}

		ddl.Specs[keyspace] = schema
	}

	spec, ok := schema[table]
	if !ok {
		return nil, &UnknownTableError{
			Keyspace: keyspace,
			Table:    table,
		}
	}

	return spec, nil
}

type SchemaInformation struct {
	Schema      *DDLSchema
	SkipColumns bool
}

func (si *SchemaInformation) Semantics(keyspace string) semantics.SchemaInformation {
	return &ddlWrapper{
		ddl:             si.Schema,
		defaultKeyspace: keyspace,
		skipColumns:     si.SkipColumns,
	}
}

type ddlWrapper struct {
	ddl             *DDLSchema
	skipColumns     bool
	defaultKeyspace string
}

var _ semantics.SchemaInformation = (*ddlWrapper)(nil)

func (d *ddlWrapper) FindTableOrVindex(tab sqlparser.TableName) (*vindexes.Table, vindexes.Vindex, string, topodatapb.TabletType, key.Destination, error) {
	destKeyspace, destTabletType, destTarget, err := topoproto.ParseDestination(tab.Qualifier.String(), topodatapb.TabletType_PRIMARY)
	if err != nil {
		return nil, nil, destKeyspace, destTabletType, destTarget, err
	}
	if destKeyspace == "" {
		destKeyspace = d.defaultKeyspace
	}

	tbl := &vindexes.Table{
		Name:     tab.Name,
		Keyspace: &vindexes.Keyspace{Name: destKeyspace, Sharded: false},
	}

	if tab.Qualifier.String() == "" && tab.Name.String() == "dual" {
		return tbl, nil, destKeyspace, destTabletType, destTarget, nil
	}

	spec, err := d.ddl.LoadTableSpec(destKeyspace, tab.Name.String())
	if err != nil {
		return nil, nil, destKeyspace, destTabletType, destTarget, err
	}

	columns := make([]vindexes.Column, len(spec.Columns))
	for idx, col := range spec.Columns {
		collationID, err := collationForColumn(spec, col)
		if err != nil {
			return nil, nil, destKeyspace, destTabletType, destTarget, err
		}
		columns[idx] = vindexes.Column{
			Name:          col.Name,
			Type:          col.Type.SQLType(),
			CollationName: collations.Local().LookupByID(collationID).Name(),
		}
	}
	// We use this to keep being able to use a hack in Singularity to get
	// proper errors from missing columns because the Gen4 planner returns
	// only opaque errors we can't inspect.
	if !d.skipColumns {
		tbl.ColumnListAuthoritative = true
		tbl.Columns = columns
	}

	return tbl, nil, destKeyspace, destTabletType, destTarget, nil
}

func (d *ddlWrapper) ConnCollation() collations.ID {
	return collations.Unknown
}

type RawDDL struct {
	Keyspace string
	SQL      string
}

func LoadExternalDDLSchema(ddls []RawDDL) (*DDLSchema, error) {
	schema := &DDLSchema{
		Specs: make(map[string]map[string]*sqlparser.TableSpec),
	}

	for _, ddl := range ddls {
		ks, ok := schema.Specs[ddl.Keyspace]
		if !ok {
			ks = make(map[string]*sqlparser.TableSpec)
			schema.Specs[ddl.Keyspace] = ks
		}

		stmt, err := sqlparser.ParseStrictDDL(ddl.SQL)
		if err != nil {
			return nil, err
		}

		switch stmt := stmt.(type) {
		case *sqlparser.CreateTable:
			ks[stmt.Table.Name.String()] = stmt.TableSpec
		default:
			return nil, fmt.Errorf("unexpected type: %T", stmt)
		}
	}
	return schema, nil
}

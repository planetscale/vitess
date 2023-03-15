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
	GetKeyspaces(ctx context.Context) ([]string, error)
	GetSchema(ctx context.Context, keyspace string) (*tabletmanagerdata.SchemaDefinition, error)
}

type globalSpec struct {
	keyspace string
	spec     *sqlparser.TableSpec
}

type DDLSchema struct {
	source      DDLSchemaSource
	specs       map[string]map[string]*sqlparser.TableSpec
	globalSpecs map[string]*globalSpec
	loaded      bool
}

func NewDDLSchema(src DDLSchemaSource) (*DDLSchema, error) {
	ddl := &DDLSchema{
		source:      src,
		specs:       make(map[string]map[string]*sqlparser.TableSpec),
		globalSpecs: make(map[string]*globalSpec),
	}
	err := ddl.loadAllTableSpecs()
	if err != nil {
		return nil, err
	}

	return ddl, nil
}

func (ddl *DDLSchema) loadAllTableSpecs() error {
	if ddl.source == nil {
		return nil
	}
	keyspaces, err := ddl.source.GetKeyspaces(context.Background())
	if err != nil {
		return err
	}

	for _, keyspace := range keyspaces {
		definition, err := ddl.source.GetSchema(context.Background(), keyspace)
		if err != nil {
			return err
		}

		schema := make(map[string]*sqlparser.TableSpec)
		for _, td := range definition.TableDefinitions {
			stmt, err := sqlparser.ParseStrictDDL(td.Schema)
			if err != nil {
				return &SyntaxError{Query: td.Schema, Err: err}
			}
			create, ok := stmt.(*sqlparser.CreateTable)
			if !ok {
				continue
			}
			name := create.Table.Name.String()
			schema[name] = create.TableSpec

			if _, ok := ddl.globalSpecs[name]; ok {
				ddl.globalSpecs[name] = nil
			} else {
				ddl.globalSpecs[name] = &globalSpec{keyspace: keyspace, spec: create.TableSpec}
			}
		}

		ddl.specs[keyspace] = schema
	}

	return nil
}

func (ddl *DDLSchema) AddSpec(keyspace, table string, spec *sqlparser.TableSpec) error {
	if ddl.specs[keyspace] == nil {
		ddl.specs[keyspace] = make(map[string]*sqlparser.TableSpec)
	}
	if _, ok := ddl.specs[keyspace][table]; ok {
		return fmt.Errorf("table %s.%s already exists", keyspace, table)
	}
	ddl.specs[keyspace][table] = spec

	if _, ok := ddl.globalSpecs[table]; ok {
		// nil marks this as still existing, but ambiguous.
		ddl.globalSpecs[table] = nil
	} else {
		ddl.globalSpecs[table] = &globalSpec{keyspace: keyspace, spec: spec}
	}

	return nil
}

func (ddl *DDLSchema) LoadTableSpec(keyspace, table string) (string, *sqlparser.TableSpec, error) {
	if keyspace == "" {
		spec, ok := ddl.globalSpecs[table]
		if !ok {
			return "", nil, &UnknownTableError{Keyspace: keyspace, Table: table}
		}
		if spec == nil {
			return "", nil, &AmbiguousTableError{Table: table}
		}
		return spec.keyspace, spec.spec, nil
	}

	schema, ok := ddl.specs[keyspace]
	if !ok {
		return "", nil, &UnknownTableError{Keyspace: keyspace, Table: table}
	}

	spec, ok := schema[table]
	if !ok {
		return "", nil, &UnknownTableError{
			Keyspace: keyspace,
			Table:    table,
		}
	}

	return keyspace, spec, nil
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

	var spec *sqlparser.TableSpec
	destKeyspace, spec, err = d.ddl.LoadTableSpec(destKeyspace, tab.Name.String())
	if err != nil {
		return nil, nil, destKeyspace, destTabletType, destTarget, err
	}
	// Ensure we update the resolved keyspace if a globally routed table was found.
	tbl.Keyspace.Name = destKeyspace

	columns := make([]vindexes.Column, len(spec.Columns))
	for idx, col := range spec.Columns {
		collationID, err := collationForColumn(spec, col)
		if err != nil {
			return nil, nil, destKeyspace, destTabletType, destTarget, err
		}
		columns[idx] = vindexes.Column{
			Name:          col.Name,
			Type:          col.Type.SQLType(),
			CollationName: collationID.Get().Name(),
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
	schema, err := NewDDLSchema(nil)
	if err != nil {
		return nil, err
	}

	for _, ddl := range ddls {
		stmt, err := sqlparser.ParseStrictDDL(ddl.SQL)
		if err != nil {
			return nil, err
		}

		switch stmt := stmt.(type) {
		case *sqlparser.CreateTable:
			err := schema.AddSpec(ddl.Keyspace, stmt.Table.Name.String(), stmt.TableSpec)
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unexpected type: %T", stmt)
		}
	}
	return schema, nil
}

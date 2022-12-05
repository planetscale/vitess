package utils

import (
	"testing"

	"vitess.io/vitess/go/boost/server/controller/boostplan"
	"vitess.io/vitess/go/vt/sqlparser"
)

func LoadExternalDDLSchema(t *testing.T, ddls []RawDDL) *boostplan.DDLSchema {
	schema := &boostplan.DDLSchema{
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
			t.Fatal(err)
		}

		switch stmt := stmt.(type) {
		case *sqlparser.CreateTable:
			ks[stmt.Table.Name.String()] = stmt.TableSpec
		default:
			t.Fatalf("unexpected type: %T", stmt)
		}
	}

	return schema
}

type RawDDL struct {
	Keyspace string
	SQL      string
}

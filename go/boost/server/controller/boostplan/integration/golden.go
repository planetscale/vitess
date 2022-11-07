package integration

import (
	"encoding/json"
	"os"
	"testing"

	"vitess.io/vitess/go/boost/server/controller/boostplan"
	"vitess.io/vitess/go/vt/sqlparser"
)

type TestQuery struct {
	Keyspace string
	SQL      string
	Error    string `json:",omitempty"`
}

type GoldenTestCase struct {
	SchemaInformation *boostplan.SchemaInformation
	Queries           []*TestQuery

	original *rawTestcase
	path     string
}

func loadExternalDDLSchema(t *testing.T, ddls []rawDDL) *boostplan.DDLSchema {
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

type rawDDL struct {
	Keyspace string
	SQL      string
}

type rawTestcase struct {
	DDL     []rawDDL
	Queries []*TestQuery
}

func LoadGoldenTest(t *testing.T, path string) *GoldenTestCase {
	var testcase rawTestcase

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := json.Unmarshal(raw, &testcase); err != nil {
		t.Fatal(err)
	}

	extschema := loadExternalDDLSchema(t, testcase.DDL)

	return &GoldenTestCase{
		SchemaInformation: &boostplan.SchemaInformation{
			Schema: extschema,
		},
		Queries:  testcase.Queries,
		original: &testcase,
		path:     path,
	}
}

func (tc *GoldenTestCase) Save(t testing.TB) {
	out, err := os.Create(tc.path)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()

	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(tc.original); err != nil {
		t.Fatal(err)
	}
}

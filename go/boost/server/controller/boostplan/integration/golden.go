package integration

import (
	"encoding/json"
	"os"
	"testing"

	"vitess.io/vitess/go/boost/server/controller/boostplan"
	"vitess.io/vitess/go/boost/server/controller/boostplan/integration/utils"
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

type rawTestcase struct {
	DDL     []utils.RawDDL
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

	extschema := utils.LoadExternalDDLSchema(t, testcase.DDL)

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

package operators_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/boost/server/controller/boostplan"
	"vitess.io/vitess/go/boost/server/controller/boostplan/operators"
	"vitess.io/vitess/go/slices2"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestComputeColumnUsage(t *testing.T) {
	schema, err := boostplan.LoadExternalDDLSchema([]boostplan.RawDDL{
		{Keyspace: "main", SQL: "create table user (id int, a bigint, name varchar(32), primary key(id))"},
		{Keyspace: "main", SQL: "create table product (id int, price bigint, primary key(id))"},
	})
	require.NoError(t, err)

	cases := []struct {
		query    string
		expected []string
	}{
		{
			query: "SELECT product.* FROM user join product on user.id = product.id",
			expected: []string{
				"`main`.`user`: id",
				"`main`.`product`: id (expanded), price (expanded)",
			},
		},
		{
			query: "SELECT * FROM user",
			expected: []string{
				"`main`.`user`: a (expanded), id (expanded), name (expanded)",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			conv := operators.NewConverter()
			stmt, err := sqlparser.Parse(tc.query)
			require.NoError(t, err)

			boostSI := boostplan.SchemaInformation{Schema: schema, SkipColumns: false}
			si := boostSI.Semantics("main")

			_, tr, err := conv.Plan(schema, si, stmt, "main", "toto")
			require.NoError(t, err)

			got := slices2.Map(tr, func(from *operators.TableReport) string { return from.String() })
			require.ElementsMatch(t, tc.expected, got)
		})
	}
}

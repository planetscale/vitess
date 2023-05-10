package worker

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/boost/boostrpc/service"

	"vitess.io/vitess/go/boost/common/xslice"
	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestVStreamSchemaMappings(t *testing.T) {
	type simpletype struct {
		Name string
		Type sql.Type
	}

	var C1 = simpletype{
		Name: "col1",
		Type: sql.Type{
			T: sqltypes.Int32,
		},
	}
	var C2 = simpletype{
		Name: "col2",
		Type: sql.Type{
			T: sqltypes.Int32,
		},
	}
	var C3 = simpletype{
		Name: "col3",
		Type: sql.Type{
			T: sqltypes.VarChar,
		},
	}
	var C4 = simpletype{
		Name: "col4",
		Type: sql.Type{
			T: sqltypes.VarChar,
		},
	}

	type testcase struct {
		Name           string
		Old            []simpletype
		New            []simpletype
		Dependent      map[string]string
		Expected       []srcmap
		DroppedQueries []string
	}

	var testcases = []testcase{
		{
			Name:     "Identity",
			Old:      []simpletype{C1, C2},
			New:      []simpletype{C1, C2},
			Expected: nil,
		},
		{
			Name: "Reordering",
			Old:  []simpletype{C1, C2},
			New:  []simpletype{C2, C1},
			Expected: []srcmap{
				{col: 1, t: sqltypes.Int32},
				{col: 0, t: sqltypes.Int32},
			},
		},
		{
			Name: "Added Column At The End",
			Old:  []simpletype{C1, C2},
			New:  []simpletype{C1, C2, C3},
			Expected: []srcmap{
				{col: 0, t: sqltypes.Int32},
				{col: 1, t: sqltypes.Int32},
			},
		},
		{
			Name: "Added Column At The Start",
			Old:  []simpletype{C1, C2},
			New:  []simpletype{C3, C1, C2},
			Expected: []srcmap{
				{col: 1, t: sqltypes.Int32},
				{col: 2, t: sqltypes.Int32},
			},
		},
		{
			Name: "Reverse order",
			Old:  []simpletype{C1, C2, C3},
			New:  []simpletype{C3, C2, C1},
			Expected: []srcmap{
				{col: 2, t: sqltypes.Int32},
				{col: 1, t: sqltypes.Int32},
				{col: 0, t: sqltypes.VarChar},
			},
		},
		{
			Name: "Drop a column in the middle",
			Old:  []simpletype{C1, C4, C3},
			New:  []simpletype{C1, C3},
			Expected: []srcmap{
				{col: 0, t: sqltypes.Int32},
				{col: -1},
				{col: 1, t: sqltypes.VarChar},
			},
		},
		{
			Name: "Drop a column at the end",
			Old:  []simpletype{C1, C2, C3, C4},
			New:  []simpletype{C1, C2, C3},
			Expected: []srcmap{
				{col: 0, t: sqltypes.Int32},
				{col: 1, t: sqltypes.Int32},
				{col: 2, t: sqltypes.VarChar},
				{col: -1},
			},
		},
		{
			Name: "Drop a column with a dependency",
			Old:  []simpletype{C1, C4, C3},
			New:  []simpletype{C1, C3},
			Expected: []srcmap{
				{col: 0, t: sqltypes.Int32},
				{col: -1},
				{col: 1, t: sqltypes.VarChar},
			},
			Dependent: map[string]string{
				C4.Name: "q1",
			},
			DroppedQueries: []string{"q1"},
		},
		{
			Name: "Unsafe type change with a dependency",
			Old:  []simpletype{C1, C4, C3},
			New: []simpletype{
				{
					Name: "col1",
					Type: sql.Type{
						T: sqltypes.VarChar,
					},
				},
				{
					Name: "col3",
					Type: sql.Type{
						T: sqltypes.VarChar,
					},
				},
			}, Expected: []srcmap{
				{col: 0, t: sqltypes.VarChar},
				{col: -1},
				{col: 1, t: sqltypes.VarChar},
			},
			Dependent: map[string]string{
				C1.Name: "q1",
			},
			DroppedQueries: []string{"q1"},
		},
		{
			Name: "Safe type change with a dependency",
			Old:  []simpletype{C1, C4, C3},
			New: []simpletype{
				{
					Name: "col1",
					Type: sql.Type{
						T: sqltypes.Int64,
					},
				},
				{
					Name: "col3",
					Type: sql.Type{
						T: sqltypes.VarChar,
					},
				},
			},
			Expected: []srcmap{
				{col: 0, t: sqltypes.Int64},
				{col: -1},
				{col: 1, t: sqltypes.VarChar},
			},
			Dependent: map[string]string{
				C1.Name: "q1",
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.Name, func(t *testing.T) {
			oldColumns := xslice.Map(tc.Old, func(c simpletype) string { return c.Name })
			oldFields := xslice.Map(tc.Old, func(c simpletype) sql.Type { return c.Type })
			newFields := xslice.Map(tc.New, func(c simpletype) *querypb.Field {
				return &querypb.Field{
					Name: c.Name,
					Type: c.Type.T,
				}
			})

			deps := map[string]*service.ExternalTableDescriptor_Dependency{}
			for k, v := range tc.Dependent {
				deps[k] = &service.ExternalTableDescriptor_Dependency{
					DependentQueries: []string{v},
				}
			}
			desc := &service.ExternalTableDescriptor{
				Columns:          oldColumns,
				Schema:           oldFields,
				DependentColumns: deps,
			}
			var dropped []string
			conflict := func(queryIDs []string) {
				dropped = append(dropped, queryIDs...)
			}
			result := computeSourceMap(desc, newFields, conflict)
			assert.Equal(t, tc.Expected, result)
			assert.ElementsMatch(t, tc.DroppedQueries, dropped)
		})
	}
}

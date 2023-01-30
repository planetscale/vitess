package flownode

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/dataflow/flownode/flownodepb"
	"vitess.io/vitess/go/boost/dataflow/state"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/sqltypes"
)

func TestProjection(t *testing.T) {
	t.Run("QueryThrough", func(t *testing.T) {
		var cases = []struct {
			name     string
			project  []string
			expected sql.Row
			column   int
			key      sql.Row
		}{
			{
				name:     "all",
				project:  []string{":0", ":1", ":2"},
				expected: sql.TestRow(1, 2, 3),
				column:   0,
				key:      sql.TestRow(1),
			},
			{
				name:     "some",
				project:  []string{":1"},
				expected: sql.TestRow(2),
				column:   0,
				key:      sql.TestRow(2),
			},
			{
				name:     "with literals",
				project:  []string{":1", "42"},
				expected: sql.TestRow(2, 42),
				column:   0,
				key:      sql.TestRow(2),
			},
			{
				name:     "literals reordered",
				project:  []string{"42", ":1"},
				expected: sql.TestRow(42, 2),
				column:   1,
				key:      sql.TestRow(2),
			},
			{
				name:     "arithmetic, literals",
				project:  []string{":1", "42", ":0 + :1"},
				expected: sql.TestRow(2, 42, 3),
				column:   0,
				key:      sql.TestRow(2),
			},
		}

		for _, tcase := range cases {
			t.Run(tcase.name, func(t *testing.T) {
				index := dataflow.IndexPair{
					Global: 0,
					Local:  0,
				}

				states := new(state.Map)
				row := sql.TestRow(1, 2, 3)
				schema := sql.TestSchema(sqltypes.Int64, sqltypes.Int64, sqltypes.Int64)

				st := state.NewMemoryState()
				st.AddKey([]int{0}, schema, nil, false)
				st.AddKey([]int{1}, schema, nil, false)

				records := []sql.Record{row.ToRecord(true)}
				st.ProcessRecords(records, dataflow.TagNone, nil)
				states.Insert(0, st)

				project, err := NewProjectFromProto(&flownodepb.Node_InternalProject{
					Src:         &index,
					Cols:        0,
					Projections: tcase.project,
				})
				require.NoError(t, err)

				remap := map[graph.NodeIdx]dataflow.IndexPair{0: index}
				project.OnCommit(0, remap)

				iter, found, mat := project.QueryThrough([]int{tcase.column}, tcase.key, new(Map), states)
				require.Truef(t, bool(mat), "QueryThrough parent should be materialized")
				require.Truef(t, found, "QueryThrough should not miss")

				var results []sql.Row
				iter.ForEach(func(row sql.Row) {
					results = append(results, row)
				})

				require.Len(t, results, 1)
				require.Equal(t, tcase.expected, results[0])
			})
		}
	})

}

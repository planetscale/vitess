package flownode

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/dataflow/state"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/sqltypes"
)

func TestProjection(t *testing.T) {
	t.Run("QueryThrough", func(t *testing.T) {
		var cases = []struct {
			name     string
			project  []string
			expected boostpb.Row
			column   int
			key      boostpb.Row
		}{
			{
				name:     "all",
				project:  []string{":0", ":1", ":2"},
				expected: boostpb.TestRow(1, 2, 3),
				column:   0,
				key:      boostpb.TestRow(1),
			},
			{
				name:     "some",
				project:  []string{":1"},
				expected: boostpb.TestRow(2),
				column:   0,
				key:      boostpb.TestRow(2),
			},
			{
				name:     "with literals",
				project:  []string{":1", "42"},
				expected: boostpb.TestRow(2, 42),
				column:   0,
				key:      boostpb.TestRow(2),
			},
			{
				name:     "literals reordered",
				project:  []string{"42", ":1"},
				expected: boostpb.TestRow(42, 2),
				column:   1,
				key:      boostpb.TestRow(2),
			},
			{
				name:     "arithmetic, literals",
				project:  []string{":1", "42", ":0 + :1"},
				expected: boostpb.TestRow(2, 42, 3),
				column:   0,
				key:      boostpb.TestRow(2),
			},
		}

		for _, tcase := range cases {
			t.Run(tcase.name, func(t *testing.T) {
				index := boostpb.IndexPair{
					Global: 0,
					Local:  0,
				}

				states := new(state.Map)
				row := boostpb.TestRow(1, 2, 3)
				schema := boostpb.TestSchema(sqltypes.Int64, sqltypes.Int64, sqltypes.Int64)

				st := state.NewMemoryState()
				st.AddKey([]int{0}, schema, nil)
				st.AddKey([]int{1}, schema, nil)

				records := []boostpb.Record{row.ToRecord(true)}
				st.ProcessRecords(&records, boostpb.TagNone)
				states.Insert(0, st)

				project, err := NewProjectFromProto(&boostpb.Node_InternalProject{
					Src:         &index,
					Cols:        0,
					Projections: tcase.project,
				})
				require.NoError(t, err)

				remap := map[graph.NodeIdx]boostpb.IndexPair{0: index}
				project.OnCommit(0, remap)

				iter, found, mat := project.QueryThrough([]int{tcase.column}, tcase.key, new(Map), states)
				require.Truef(t, bool(mat), "QueryThrough parent should be materialized")
				require.Truef(t, found, "QueryThrough should not miss")

				var results []boostpb.Row
				iter.ForEach(func(row boostpb.Row) {
					results = append(results, row)
				})

				require.Len(t, results, 1)
				require.Equal(t, tcase.expected, results[0])
			})
		}
	})

}

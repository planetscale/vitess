package boostpb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vthash"
)

func BenchmarkRowHashing(b *testing.B) {
	var cases = []struct {
		row    []sqltypes.Value
		schema []Type
	}{
		{
			row:    []sqltypes.Value{sqltypes.NewInt64(420)},
			schema: []Type{{T: sqltypes.Int64}},
		},
		{
			row:    []sqltypes.Value{sqltypes.NewVarChar("foobar")},
			schema: []Type{{T: sqltypes.VarChar, Collation: collations.CollationUtf8mb4ID}},
		},
		{
			row:    []sqltypes.Value{sqltypes.NewVarChar("foobar")},
			schema: []Type{{T: sqltypes.VarChar, Collation: collations.CollationBinaryID}},
		},
	}

	for i, tc := range cases {
		b.Run(fmt.Sprintf("%d_alloc", i), func(b *testing.B) {
			var row = RowFromVitess(tc.row)
			var schema = tc.schema

			b.ReportAllocs()
			b.ResetTimer()

			for n := 0; n < b.N; n++ {
				var hasher vthash.Hasher
				_ = row.Hash(&hasher, schema)
			}
		})

		b.Run(fmt.Sprintf("%d_reuse", i), func(b *testing.B) {
			var hasher vthash.Hasher
			var row = RowFromVitess(tc.row)
			var schema = tc.schema

			b.ReportAllocs()
			b.ResetTimer()

			for n := 0; n < b.N; n++ {
				_ = row.Hash(&hasher, schema)
			}
		})
	}
}

func TestRowHashing(t *testing.T) {
	var cases = []struct {
		row1      []sqltypes.Value
		row2      []sqltypes.Value
		key       []int
		schema    []Type
		different bool
	}{
		{
			row1:   []sqltypes.Value{sqltypes.NewInt64(420)},
			row2:   []sqltypes.Value{sqltypes.NewInt32(420)},
			schema: []Type{{T: sqltypes.Int64}},
		},
		{
			row1:   []sqltypes.Value{sqltypes.NewVarChar("foobar")},
			row2:   []sqltypes.Value{sqltypes.NewVarBinary("FOOBAR")},
			schema: []Type{{T: sqltypes.VarChar, Collation: collations.CollationUtf8mb4ID}},
		},
		{
			row1:      []sqltypes.Value{sqltypes.NewVarChar("foobar")},
			row2:      []sqltypes.Value{sqltypes.NewVarBinary("FOOBAR")},
			schema:    []Type{{T: sqltypes.VarChar, Collation: collations.CollationBinaryID}},
			different: true,
		},
	}

	for _, tc := range cases {
		var r1 = RowFromVitess(tc.row1)
		var r2 = RowFromVitess(tc.row2)

		var hasher vthash.Hasher
		var h1, h2 vthash.Hash
		if tc.key == nil {
			h1 = r1.Hash(&hasher, tc.schema)
			h2 = r2.Hash(&hasher, tc.schema)
		} else {
			h1 = r1.HashWithKeySchema(&hasher, tc.key, tc.schema)
			h2 = r2.HashWithKeySchema(&hasher, tc.key, tc.schema)
		}

		if !tc.different && h1 != h2 {
			t.Errorf("expected %v and %v to hash to the same value (got %x vs %x)",
				tc.row1, tc.row2, h1, h2,
			)
		}
		if tc.different && h1 == h2 {
			t.Errorf("expected %v and %v to hash to different values (got %x)",
				tc.row1, tc.row2, h1,
			)
		}
	}
}

func TestRowSerialization(t *testing.T) {
	const MaxSize = 8

	for Size := 0; Size < MaxSize; Size++ {
		var vtrow []sqltypes.Value
		for i := 0; i < Size; i++ {
			vtrow = append(vtrow, sqltypes.NewInt64(int64(i)))
		}

		row := RowFromVitess(vtrow)
		vs := row.ToValues()

		assert.Equal(t, row.Len(), Size)
		assert.Equal(t, row.Len(), len(vs))
		assert.Equal(t, row.Len(), len(vtrow))

		for i, v := range vs {
			assert.Equal(t, vtrow[i], v.ToVitessUnsafe())
			assert.Equal(t, vtrow[i], v.ToVitess())
		}

		for i := 0; i < Size; i++ {
			assert.Equal(t, vtrow[i], row.ValueAt(i).ToVitessUnsafe())
			assert.Equal(t, vtrow[i], row.ValueAt(i).ToVitess())
		}

		vs = ValuesFromVitess(vtrow)
		row = RowFromValues(vs)

		assert.Equal(t, row.Len(), Size)
		assert.Equal(t, row.Len(), len(vs))
		assert.Equal(t, row.Len(), len(vtrow))

		for i, v := range vs {
			assert.Equal(t, vtrow[i], v.ToVitessUnsafe())
			assert.Equal(t, vtrow[i], v.ToVitess())
		}

		for i := 0; i < Size; i++ {
			assert.Equal(t, vtrow[i], row.ValueAt(i).ToVitessUnsafe())
			assert.Equal(t, vtrow[i], row.ValueAt(i).ToVitess())
		}

		vs = row.ToValues()
		row = RowFromValues(vs)

		assert.Equal(t, row.Len(), Size)
		assert.Equal(t, row.Len(), len(vs))
		assert.Equal(t, row.Len(), len(vtrow))

		for i, v := range vs {
			assert.Equal(t, vtrow[i], v.ToVitessUnsafe())
			assert.Equal(t, vtrow[i], v.ToVitess())
		}

		for i := 0; i < Size; i++ {
			assert.Equal(t, vtrow[i], row.ValueAt(i).ToVitessUnsafe())
			assert.Equal(t, vtrow[i], row.ValueAt(i).ToVitess())
		}
	}
}

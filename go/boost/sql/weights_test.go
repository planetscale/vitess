package sql

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

func TestVarWeightString(t *testing.T) {
	var cases = []struct {
		row    Row
		schema []Type
		weight []byte
	}{
		{TestRow(1), TestSchema(sqltypes.Int64), []byte{0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1}},
		{TestRow(0xf8), TestSchema(sqltypes.Int64), []byte{0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8}},
		{TestRow([]byte{0xf8}), TestSchema(sqltypes.VarBinary), []byte{0xf8, 0xff, 0xf8, 0x01}},
		{TestRow([]byte{0x00}, []byte{0x00}), TestSchema(sqltypes.VarBinary, sqltypes.VarBinary), []byte{0x00, 0xf8, 0x01, 0x00, 0xf8, 0x01}},
		{TestRow([]byte{0x00}, []byte{0x00, 0xf8, 0x00}), TestSchema(sqltypes.VarBinary, sqltypes.VarBinary), []byte{0x00, 0xf8, 0x01, 0x00, 0xf8, 0xff, 0x00, 0xf8, 0x01}},
	}

	for _, tc := range cases {
		t.Run(tc.row.GoString(), func(t *testing.T) {
			w, err := tc.row.Weights(tc.schema)
			require.NoError(t, err)
			require.Equal(t, tc.weight, []byte(w))
		})
	}
}

func TestWeightStrings(t *testing.T) {
	const Length = 1000

	type item struct {
		value  sqltypes.Value
		weight string
	}

	var cases = []struct {
		name string
		gen  func() sqltypes.Value
		t    Type
	}{
		{"int64", randomInt64, Type{T: sqltypes.Int64, Collation: collations.CollationBinaryID}},
		{"uint64", randomUint64, Type{T: sqltypes.Uint64, Collation: collations.CollationBinaryID}},
		{"float64", randomFloat64, Type{T: sqltypes.Float64, Collation: collations.CollationBinaryID}},
		{"varchar", randomVarChar, Type{T: sqltypes.VarChar, Collation: collations.CollationUtf8mb4ID}},
		{"varbinary", randomVarBinary, Type{T: sqltypes.VarBinary, Collation: collations.CollationBinaryID}},
		{"decimal", randomDecimal, Type{T: sqltypes.Decimal, Collation: collations.CollationBinaryID, Length: 20, Precision: 10}},
		{"json", randomJSON, Type{T: sqltypes.TypeJSON, Collation: collations.CollationBinaryID}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			items := make([]item, 0, Length)
			for i := 0; i < Length; i++ {
				v := tc.gen()
				w, _, err := WeightString(nil, v, tc.t)
				require.NoError(t, err)

				items = append(items, item{value: v, weight: string(w)})
			}

			slices.SortFunc(items, func(a, b item) int {
				// TODO: switch to cmp.Compare for Go 1.21+.
				//
				// https://pkg.go.dev/cmp@master#Compare.
				switch {
				case a.weight < b.weight:
					return -1
				case a.weight > b.weight:
					return 1
				default:
					return 0
				}
			})

			for i := 0; i < Length-1; i++ {
				a := items[i]
				b := items[i+1]

				cmp, err := evalengine.NullsafeCompare(a.value, b.value, tc.t.Collation)
				require.NoError(t, err)

				if cmp > 0 {
					t.Fatalf("expected %v [pos=%d] to come after %v [pos=%d]\nav = %v\nbv = %v",
						a.value, i, b.value, i+1,
						[]byte(a.weight), []byte(b.weight),
					)
				}
			}
		})
	}
}

func randomInt64() sqltypes.Value {
	i := rand.Int63()
	if rand.Int()&0x1 == 1 {
		i = -i
	}
	return sqltypes.NewInt64(i)
}
func randomUint64() sqltypes.Value    { return sqltypes.NewUint64(rand.Uint64()) }
func randomVarChar() sqltypes.Value   { return sqltypes.NewVarChar(string(randomBytes())) }
func randomVarBinary() sqltypes.Value { return sqltypes.NewVarBinary(string(randomBytes())) }
func randomFloat64() sqltypes.Value {
	return sqltypes.NewFloat64(rand.NormFloat64())
}
func randomDecimal() sqltypes.Value {
	dec := fmt.Sprintf("%d.%d", rand.Intn(9999999999), rand.Intn(9999999999))
	if rand.Int()&0x1 == 1 {
		dec = "-" + dec
	}
	return sqltypes.NewDecimal(dec)
}

func randomBytes() []byte {
	const Dictionary = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	b := make([]byte, 4+rand.Intn(256))
	for i := range b {
		b[i] = Dictionary[rand.Intn(len(Dictionary))]
	}
	return b
}

func randomJSON() sqltypes.Value {
	var j string
	switch rand.Intn(6) {
	case 0:
		j = "null"
	case 1:
		i := rand.Int63()
		if rand.Int()&0x1 == 1 {
			i = -i
		}
		j = strconv.FormatInt(i, 10)
	case 2:
		j = strconv.FormatFloat(rand.NormFloat64(), 'g', -1, 64)
	case 3:
		j = strconv.Quote(string(randomBytes()))
	case 4:
		j = "true"
	case 5:
		j = "false"
	}
	v, err := sqltypes.NewJSON(j)
	if err != nil {
		panic(err)
	}
	return v
}

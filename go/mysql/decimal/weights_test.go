package decimal

import (
	"bytes"
	"math/rand"
	"strconv"
	"testing"

	"golang.org/x/exp/slices"
)

func TestWeightStrings(t *testing.T) {
	randDecimal := func() Decimal {
		var buf []byte
		if rand.Intn(2) == 1 {
			buf = append(buf, '-')
		}
		buf = strconv.AppendUint(buf, rand.Uint64()%10000000000, 10)
		buf = append(buf, '.')
		buf = strconv.AppendUint(buf, rand.Uint64()%10000000000, 10)

		d, err := NewFromMySQL(buf)
		if err != nil {
			t.Fatalf("failed to parse decimal %q: %v", buf, err)
		}
		return d
	}

	type item struct {
		dec    Decimal
		weight []byte
	}

	const Length = 100
	items := make([]item, 0, Length)

	for i := 0; i < Length; i++ {
		dec := randDecimal()
		i := item{
			dec:    dec,
			weight: dec.WeightString(nil, 20, 10),
		}
		items = append(items, i)
	}

	slices.SortFunc(items, func(a, b item) bool {
		return bytes.Compare(a.weight, b.weight) < 0
	})

	for i := 0; i < Length-1; i++ {
		a := items[i]
		b := items[i+1]

		cmp := a.dec.Cmp(b.dec)
		if cmp > 0 {
			t.Fatalf("expected %v [pos=%d] to come after %v [pos=%d]\naw: %v\nbw: %v",
				a.dec.String(), i, b.dec.String(), i+1,
				a.weight, b.weight,
			)
		}
	}

}

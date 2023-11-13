package endtoend

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"vitess.io/vitess/go/mysql"
)

// 10 groups, 119 characters
const cValueTemplate = "###########-###########-###########-" +
	"###########-###########-###########-" +
	"###########-###########-###########-" +
	"###########"

// 5 groups, 59 characters
const padValueTemplate = "###########-###########-###########-" +
	"###########-###########"

func sysbenchRandom(rng *rand.Rand, template string) []byte {
	out := make([]byte, 0, len(template))
	for i := range template {
		switch template[i] {
		case '#':
			out = append(out, '0'+byte(rng.Intn(10)))
		default:
			out = append(out, template[i])
		}
	}
	return out
}

var oltpInitOnce sync.Once

const OLTPMaxRows = 10000
const OLTPRangeSize = 100

func oltpSimpleRanges(b *testing.B, rng *rand.Rand, query *bytes.Buffer, conn *mysql.Conn) {
	query.Reset()

	id := rng.Intn(OLTPMaxRows)
	_, _ = fmt.Fprintf(query, "SELECT c FROM oltp_test WHERE id BETWEEN %d AND %d", id, id+rng.Intn(OLTPRangeSize)-1)
	_, err := conn.ExecuteFetch(query.String(), 1000, false)
	if err != nil {
		b.Fatal(err)
	}
}

func oltpDistinctRanges(b *testing.B, rng *rand.Rand, query *bytes.Buffer, conn *mysql.Conn) {
	query.Reset()

	id := rng.Intn(OLTPMaxRows)
	_, _ = fmt.Fprintf(query, "SELECT DISTINCT c FROM oltp_test WHERE id BETWEEN %d AND %d ORDER BY c", id, id+rng.Intn(OLTPRangeSize)-1)
	_, err := conn.ExecuteFetch(query.String(), 1000, false)
	if err != nil {
		b.Error(err)
	}
}

func oltpOrderRanges(b *testing.B, rng *rand.Rand, query *bytes.Buffer, conn *mysql.Conn) {
	query.Reset()

	id := rng.Intn(OLTPMaxRows)
	_, _ = fmt.Fprintf(query, "SELECT c FROM oltp_test WHERE id BETWEEN %d AND %d ORDER BY c", id, id+rng.Intn(OLTPRangeSize)-1)
	_, err := conn.ExecuteFetch(query.String(), 1000, false)
	if err != nil {
		b.Fatal(err)
	}
}

func oltpSumRanges(b *testing.B, rng *rand.Rand, query *bytes.Buffer, conn *mysql.Conn) {
	query.Reset()

	id := rng.Intn(OLTPMaxRows)
	_, _ = fmt.Fprintf(query, "SELECT SUM(k) FROM oltp_test WHERE id BETWEEN %d AND %d", id, id+rng.Intn(OLTPRangeSize)-1)
	_, err := conn.ExecuteFetch(query.String(), 1000, false)
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkOLTP(b *testing.B) {
	rng := rand.New(rand.NewSource(1234))

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	var query bytes.Buffer

	oltpInitOnce.Do(func() {
		b.Logf("seeding database for benchmark...")

		var rows int = 1
		for i := 0; i < OLTPMaxRows/10; i++ {
			query.Reset()
			query.WriteString("insert into oltp_test(id, k, c, pad) values ")
			for j := 0; j < 10; j++ {
				if j > 0 {
					query.WriteString(", ")
				}
				_, _ = fmt.Fprintf(&query, "(%d, %d, '%s', '%s')", rows, rng.Int31n(0xFFFF), sysbenchRandom(rng, cValueTemplate), sysbenchRandom(rng, padValueTemplate))
				rows++
			}

			_, err = conn.ExecuteFetch(query.String(), -1, false)
			if err != nil {
				b.Fatal(err)
			}
		}
		b.Logf("finshed (inserted %d rows)", rows)
	})

	b.Run("SimpleRanges", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			oltpSimpleRanges(b, rng, &query, conn)
		}
	})

	b.Run("SumRanges", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			oltpSumRanges(b, rng, &query, conn)
		}
	})

	b.Run("OrderRanges", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			oltpOrderRanges(b, rng, &query, conn)
		}
	})

	b.Run("DistinctRanges", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			oltpDistinctRanges(b, rng, &query, conn)
		}
	})

	b.Run("Random", func(b *testing.B) {
		perform := []func(b *testing.B, rng *rand.Rand, query *bytes.Buffer, conn *mysql.Conn){
			oltpSimpleRanges, oltpSumRanges, oltpOrderRanges, oltpDistinctRanges,
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			perform[rand.Intn(len(perform))](b, rng, &query, conn)
		}
	})
}

package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"sync/atomic"

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

func sysbenchRandom(template string) []byte {
	out := make([]byte, 0, len(template))
	for i := range template {
		switch template[i] {
		case '#':
			out = append(out, '0'+byte(rand.IntN(10)))
		default:
			out = append(out, template[i])
		}
	}
	return out
}

type OLTP struct {
	MaxRows   int
	RangeSize int
	Requests  atomic.Uint64
}

func (b *OLTP) Init(conn *mysql.Conn) error {
	log.Printf("seeding database for benchmark...")

	var query bytes.Buffer
	var rows int = 1
	for i := 0; i < b.MaxRows/10; i++ {
		query.Reset()
		query.WriteString("insert into oltp_test(id, k, c, pad) values ")
		for j := 0; j < 10; j++ {
			if j > 0 {
				query.WriteString(", ")
			}
			_, _ = fmt.Fprintf(&query, "(%d, %d, '%s', '%s')", rows, rand.Int32N(0xFFFF), sysbenchRandom(cValueTemplate), sysbenchRandom(padValueTemplate))
			rows++
		}

		_, err := conn.ExecuteFetch(query.String(), -1, false)
		if err != nil {
			return err
		}
	}
	log.Printf("finshed (inserted %d rows)", rows)
	return nil
}

func (b *OLTP) SimpleRanges(ctx context.Context, conn *mysql.Conn) error {
	var query bytes.Buffer
	for ctx.Err() == nil {
		id := rand.IntN(b.MaxRows)

		query.Reset()
		_, _ = fmt.Fprintf(&query, "SELECT c FROM oltp_test WHERE id BETWEEN %d AND %d", id, id+rand.IntN(b.RangeSize)-1)
		_, err := conn.ExecuteFetch(query.String(), 1000, false)
		if err != nil {
			return err
		}
		b.Requests.Add(1)
	}
	return nil
}

func (b *OLTP) SumRanges(ctx context.Context, conn *mysql.Conn) error {
	var query bytes.Buffer
	for ctx.Err() == nil {
		id := rand.IntN(b.MaxRows)

		query.Reset()
		_, _ = fmt.Fprintf(&query, "SELECT SUM(k) FROM oltp_test WHERE id BETWEEN %d AND %d", id, id+rand.IntN(b.RangeSize)-1)
		_, err := conn.ExecuteFetch(query.String(), 1000, false)
		if err != nil {
			return err
		}
		b.Requests.Add(1)
	}
	return nil
}

func (b *OLTP) OrderRanges(ctx context.Context, conn *mysql.Conn) error {
	var query bytes.Buffer
	for ctx.Err() == nil {
		id := rand.IntN(b.MaxRows)

		query.Reset()
		_, _ = fmt.Fprintf(&query, "SELECT c FROM oltp_test WHERE id BETWEEN %d AND %d ORDER BY c", id, id+rand.IntN(b.RangeSize)-1)
		_, err := conn.ExecuteFetch(query.String(), 1000, false)
		if err != nil {
			return err
		}
		b.Requests.Add(1)
	}
	return nil
}

func (b *OLTP) DistinctRanges(ctx context.Context, conn *mysql.Conn) error {
	var query bytes.Buffer
	for ctx.Err() == nil {
		id := rand.IntN(b.MaxRows)

		query.Reset()
		_, _ = fmt.Fprintf(&query, "SELECT DISTINCT c FROM oltp_test WHERE id BETWEEN %d AND %d ORDER BY c", id, id+rand.IntN(b.RangeSize)-1)
		_, err := conn.ExecuteFetch(query.String(), 1000, false)
		if err != nil {
			return err
		}
		b.Requests.Add(1)
	}
	return nil
}

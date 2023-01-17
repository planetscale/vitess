package benchmark

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"vitess.io/vitess/go/boost/test/helpers/booste2e"
)

var benchmarkRecipe = flag.String("benchmark-recipe", "", "")
var benchmarkDuration = flag.Duration("benchmark-duration", 30*time.Second, "")

type Benchmark interface {
	Write(ctx context.Context, tt *booste2e.Test) error
	Read(ctx context.Context, tt *booste2e.Test) error
	Verify(ctx context.Context, tt *booste2e.Test) error
	Report(w io.Writer)
}

type votesBenchmark struct {
	votes    atomic.Int64
	articles atomic.Int64
}

func (bench *votesBenchmark) Report(w io.Writer) {
	fmt.Fprintf(w, "votes: %d / articles: %d", bench.votes.Load(), bench.articles.Load())
}

func (bench *votesBenchmark) Write(ctx context.Context, tt *booste2e.Test) error {
	var wg errgroup.Group

	wg.Go(func() error {
		conn := tt.Conn()

		for ctx.Err() == nil {
			query := fmt.Sprintf(
				`INSERT INTO article (id, title) VALUES
				(%[1]d + 0, 'Article a-%[1]d'),
				(%[1]d + 1, 'Article b-%[1]d'),
				(%[1]d + 2, 'Article c-%[1]d'),
				(%[1]d + 3, 'Article d-%[1]d'),
				(%[1]d + 4, 'Article e-%[1]d'),
				(%[1]d + 5, 'Article f-%[1]d'),
				(%[1]d + 6, 'Article g-%[1]d'),
				(%[1]d + 7, 'Article h-%[1]d'),
				(%[1]d + 8, 'Article i-%[1]d'),
				(%[1]d + 9, 'Article j-%[1]d')`, bench.articles.Load()+1)
			_, err := conn.ExecuteFetch(query, 0, false)
			if err != nil {
				return err
			}

			bench.articles.Add(10)
		}
		return nil
	})

	wg.Go(func() error {
		conn := tt.Conn()

		for ctx.Err() == nil && bench.articles.Load() < 100 {
			runtime.Gosched()
		}

		for ctx.Err() == nil {
			curvotes := bench.articles.Load()
			query := fmt.Sprintf(
				`INSERT INTO vote (article_id, user) VALUES
				(%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d),
				(%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d)`,
				rand.Intn(int(curvotes))+1, rand.Intn(10000)+1,
				rand.Intn(int(curvotes))+1, rand.Intn(10000)+1,
				rand.Intn(int(curvotes))+1, rand.Intn(10000)+1,
				rand.Intn(int(curvotes))+1, rand.Intn(10000)+1,
				rand.Intn(int(curvotes))+1, rand.Intn(10000)+1,
				rand.Intn(int(curvotes))+1, rand.Intn(10000)+1,
				rand.Intn(int(curvotes))+1, rand.Intn(10000)+1,
				rand.Intn(int(curvotes))+1, rand.Intn(10000)+1,
				rand.Intn(int(curvotes))+1, rand.Intn(10000)+1,
				rand.Intn(int(curvotes))+1, rand.Intn(10000)+1,
			)

			_, err := conn.ExecuteFetch(query, 0, false)
			if err != nil {
				return err
			}

			bench.votes.Add(10)
		}
		return nil
	})

	return wg.Wait()
}

func (*votesBenchmark) Read(ctx context.Context, tt *booste2e.Test) error {
	return nil
}

func (*votesBenchmark) Verify(ctx context.Context, tt *booste2e.Test) error {
	conn := tt.Conn()

	const selectQuery = `
	SELECT article.id, article.title, votecount.votes AS votes
		FROM article
		LEFT JOIN (SELECT vote.article_id, COUNT(vote.user) AS votes
				   FROM vote GROUP BY vote.article_id) AS votecount
		ON (article.id = votecount.article_id) WHERE article.id = %d;
`

	_, err := conn.ExecuteFetch("SET @@boost_cached_queries = true", -1, false)
	if err != nil {
		return err
	}

	var failed []int
	for i := 1; i <= 333; i += 7 {
		rs, err := conn.ExecuteFetch(fmt.Sprintf(selectQuery, i), -1, false)
		if err != nil {
			failed = append(failed, i)
			continue
		}
		tt.Logf("article.id = %03d | %v", i, rs.Rows)
	}
	tt.Logf("retrying %d timed out queries", len(failed))
	for _, i := range failed {
		rs, err := conn.ExecuteFetch(fmt.Sprintf(selectQuery, i), -1, false)
		if err != nil {
			tt.Errorf("failed after retry: %v", err)
			continue
		}
		tt.Logf("article.id = %03d | %v", i, rs.Rows)
	}
	return nil
}

func TestBoostBenchmark(t *testing.T) {
	if *benchmarkRecipe == "" {
		t.Skipf("no benchmark-recipe set")
	}

	var bench Benchmark
	switch *benchmarkRecipe {
	case "votes":
		bench = &votesBenchmark{}
	default:
		t.Fatalf("unknown benchmark: %q", *benchmarkRecipe)
	}

	tt := booste2e.Setup(t, booste2e.WithBoostInstance("--pprof=cpu,path=."), booste2e.WithRecipe(*benchmarkRecipe))

	ctx, cancel := context.WithTimeout(context.Background(), *benchmarkDuration)
	defer cancel()

	var wg errgroup.Group

	wg.Go(func() error {
		return bench.Read(ctx, tt)
	})
	wg.Go(func() error {
		return bench.Write(ctx, tt)
	})
	wg.Go(func() error {
		var buf bytes.Buffer
		tick := time.NewTicker(1 * time.Second)
		defer tick.Stop()

		for {
			select {
			case <-ctx.Done():
				return nil
			case <-tick.C:
				bench.Report(&buf)
				t.Logf("[BENCHMARK] %s", buf.Bytes())
				buf.Reset()
			}
		}
	})

	if err := wg.Wait(); err != nil {
		t.Fatalf("benchmark failed: %v", err)
	}

	if err := bench.Verify(context.Background(), tt); err != nil {
		t.Fatalf("verification failed: %v", err)
	}
}

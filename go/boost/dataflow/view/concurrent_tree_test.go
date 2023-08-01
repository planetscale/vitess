package view

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/boost/server/controller/boostplan/viewplan"
	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/slices2"
	"vitess.io/vitess/go/sqltypes"
)

func lookupRange(r *TreeReader, bounds Bounds) (found []sql.Row) {
	r.LookupRange(bounds, func(rows Rows) { found = rows.Collect(found) })
	return
}

func testPlan(key []int) *viewplan.Plan {
	return &viewplan.Plan{
		InternalStateKey: key,
		ExternalStateKey: key,
	}
}

func bounds(t *testing.T, r *TreeReader) (func(...any) Bound, func(...any) Bound) {
	bound := func(inclusive bool, cols ...any) Bound {
		t.Helper()
		w, err := sql.TestRow(cols...).Weights(r.keySchema)
		if err != nil {
			t.Fatalf("failed to compute bound: %v", err)
		}
		return Bound{weight: w, inclusive: inclusive}
	}

	return func(cols ...any) Bound {
			return bound(true, cols...)
		}, func(cols ...any) Bound {
			return bound(false, cols...)
		}
}

func TestTreeIteration(t *testing.T) {
	r, w := newTreeView(testPlan([]int{0}), sql.TestSchema(sqltypes.Int64, sqltypes.VarChar), nil)
	inclusive, exclusive := bounds(t, r)

	defer func() {
		w.Free()
	}()

	var records []sql.Record
	for i := 0; i < 100; i++ {
		records = append(records, sql.TestRow(i, fmt.Sprintf("record-%d", i)).ToRecord(true))
	}

	w.Add(records)
	w.Swap()

	found := lookupRange(r, Bounds{Lower: inclusive(0), Upper: inclusive(69)})
	assert.Len(t, found, 70)
	assert.Equal(t, `[INT64(0) VARCHAR("record-0")]`, found[0].String())
	assert.Equal(t, `[INT64(69) VARCHAR("record-69")]`, found[69].String())

	found = lookupRange(r, Bounds{Lower: exclusive(11), Upper: exclusive(13)})
	assert.Len(t, found, 1)
	assert.Equal(t, `[INT64(12) VARCHAR("record-12")]`, found[0].String())

	found = lookupRange(r, Bounds{})
	assert.Len(t, found, 100)

	found = lookupRange(r, Bounds{Upper: inclusive(10)})
	assert.Len(t, found, 11)

	found = lookupRange(r, Bounds{Lower: exclusive(10)})
	assert.Len(t, found, 89)
	assert.Equal(t, `[INT64(11) VARCHAR("record-11")]`, found[0].String())
}

func TestTreeIterationWithVariablePrefix(t *testing.T) {
	r, w := newTreeView(testPlan([]int{0, 1}), sql.TestSchema(sqltypes.VarChar, sqltypes.Int64), nil)
	inclusive, _ := bounds(t, r)

	defer func() {
		w.Free()
	}()

	records := []sql.Row{
		sql.TestRow("a", 1),
		sql.TestRow("a", 2),
		sql.TestRow("a", 3),
		sql.TestRow("aaaa", 1),
		sql.TestRow("aaaa", 2),
		sql.TestRow("aaaa", 3),
		sql.TestRow("aaaabbbb", 1),
		sql.TestRow("aaaabbbb", 2),
		sql.TestRow("aaaabbbb", 3),
		sql.TestRow("aa", 0),
		sql.TestRow("aaabbb", 0),
	}

	w.Add(slices2.Map(records, func(r sql.Row) sql.Record { return r.AsRecord() }))
	w.Swap()

	found := lookupRange(r, Bounds{Lower: inclusive("aaaa", 1), Upper: inclusive("aaaa", 4)})
	assert.Len(t, found, 3)
	t.Logf("found: %v", found)

	found = lookupRange(r, Bounds{Lower: inclusive("aaaabbbb", 1), Upper: inclusive("aaaabbbb", 4)})
	assert.Len(t, found, 3)
}

func TestTreeIterationWithVariablePrefixConflict(t *testing.T) {
	r, w := newTreeView(testPlan([]int{0, 1}), sql.TestSchema(sqltypes.VarBinary, sqltypes.VarBinary), nil)
	inclusive, _ := bounds(t, r)

	defer func() {
		w.Free()
	}()

	records := []sql.Row{
		sql.TestRow([]byte("aaaa"), []byte("bbbb")),
		sql.TestRow([]byte("aaaab"), []byte("bbb")),
	}

	w.Add(slices2.Map(records, func(r sql.Row) sql.Record { return r.AsRecord() }))
	w.Swap()

	found := lookupRange(r, Bounds{Lower: inclusive([]byte("aaaa"), []byte("bbbb")), Upper: inclusive([]byte("aaaa"), []byte("cccc"))})
	assert.Len(t, found, 1)
	t.Logf("found: %v", found)
}

package view

import (
	"sync"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/boost/common/rowstore/offheap"
	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vthash"
)

func TestStoreWorks(t *testing.T) {
	var hasher vthash.Hasher
	a := sql.TestRow(1, "a")
	r, w := newMapView(testPlan([]int{0}), sql.TestSchema(sqltypes.Int64, sqltypes.VarChar), nil)

	defer func() {
		w.Free()
		offheap.DefaultAllocator.EnsureNoLeaks()
	}()

	hit := r.Lookup(&hasher, a.Slice(0, 1), func(r Rows) {
		assert.Equal(t, 0, r.Len())
	})
	assert.True(t, hit)

	w.Add(a.AsRecords())

	hit = r.Lookup(&hasher, a.Slice(0, 1), func(r Rows) {
		assert.Equal(t, 0, r.Len())
	})
	assert.True(t, hit)

	w.Swap()

	hit = r.Lookup(&hasher, a.Slice(0, 1), func(r Rows) {
		assert.Equal(t, 1, r.Len())
	})
	assert.True(t, hit)
}

func TestMinimalQuery(t *testing.T) {
	a := sql.TestRow(1, "a")
	b := sql.TestRow(1, "b")
	r, w := newMapView(testPlan([]int{0}), sql.TestSchema(sqltypes.Int64, sqltypes.VarChar), nil)

	defer func() {
		w.Free()
		offheap.DefaultAllocator.EnsureNoLeaks()
	}()

	w.Add(a.AsRecords())
	w.Add(b.AsRecords())
	w.Swap()

	var hasher vthash.Hasher

	hit := r.Lookup(&hasher, a.Slice(0, 1), func(r Rows) {
		assert.Equal(t, 2, r.Len())
	})
	assert.True(t, hit)

	hit = r.Lookup(&hasher, a.Slice(0, 1), func(r Rows) {
		allrows := r.Collect(nil)
		assert.Contains(t, allrows, a)
		assert.Contains(t, allrows, b)
	})
	assert.True(t, hit)
}

func TestBusy(t *testing.T) {
	const N = 1000
	var wg sync.WaitGroup

	r, w := newMapView(testPlan([]int{0}), sql.TestSchema(sqltypes.Int64), nil)

	defer func() {
		w.Free()
		offheap.DefaultAllocator.EnsureNoLeaks()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for n := int64(0); n < N; n++ {
			w.Add(sql.TestRow(n).AsRecords())
			w.Swap()
		}
	}()

	var hasher vthash.Hasher
	for n := int64(0); n < N; n++ {
		key := sql.RowFromVitess([]sqltypes.Value{sqltypes.NewInt64(n)})
	stress:
		for {
			var foundlen int
			hit := r.Lookup(&hasher, key, func(r Rows) { foundlen = r.Len() })
			switch {
			case !hit:
				continue
			case foundlen == 1:
				break stress
			}
		}
	}

	wg.Wait()
}

func assertLookup(t *testing.T, r *MapReader, key sql.Row, wantRowsLen int, wantHit bool) {
	t.Helper()

	var hasher vthash.Hasher
	hit := r.Lookup(&hasher, key, func(rows Rows) {
		require.Equal(t, wantRowsLen, rows.Len())
	})
	require.Equal(t, wantHit, hit)
}

func TestConcurrentRows(t *testing.T) {
	r, w := newMapView(testPlan([]int{0}), sql.TestSchema(sqltypes.Int64, sqltypes.Int64), func(iterator []sql.Row) bool {
		return true
	})

	defer func() {
		w.Free()
		offheap.DefaultAllocator.EnsureNoLeaks()
	}()

	w.Add([]sql.Record{
		sql.TestRow(1, 1).ToRecord(true),
		sql.TestRow(1, 2).ToRecord(true),
		sql.TestRow(1, 3).ToRecord(true),
		sql.TestRow(1, 4).ToRecord(true),
	})

	assertLookup(t, r, sql.TestRow(1), 0, false)
	w.Swap()
	w.Add([]sql.Record{sql.TestRow(1, 2).ToRecord(false)})
	assertLookup(t, r, sql.TestRow(1), 4, true)
	w.Swap()
	assertLookup(t, r, sql.TestRow(1), 3, true)
	w.Add([]sql.Record{sql.TestRow(1, 1).ToRecord(false)})
	assertLookup(t, r, sql.TestRow(1), 3, true)
	w.Swap()
	assertLookup(t, r, sql.TestRow(1), 2, true)
	w.WithKey(sql.TestRow(1)).MarkFilled()
	assertLookup(t, r, sql.TestRow(1), 2, true)
	w.Swap()
	assertLookup(t, r, sql.TestRow(1), 0, true)
}

func TestConcurrentRowsInternal(t *testing.T) {
	key := []int{0}
	schema := sql.TestSchema(sqltypes.Int64, sqltypes.Int64)
	r, w := newMapView(testPlan(key), schema, func(iterator []sql.Row) bool {
		return true
	})

	defer func() {
		w.Free()
		offheap.DefaultAllocator.EnsureNoLeaks()
	}()

	w.Add([]sql.Record{
		sql.TestRow(1, 1).ToRecord(true),
		sql.TestRow(1, 2).ToRecord(true),
		sql.TestRow(1, 3).ToRecord(true),
		sql.TestRow(1, 4).ToRecord(true),
	})

	assertLookup(t, r, sql.TestRow(1), 0, false)
	w.Swap()

	row := sql.TestRow(1, 2)
	w.Add([]sql.Record{row.ToRecord(false)})

	var hasher vthash.Hasher
	h := row.HashWithKey(&hasher, key, schema)
	st := w.store.(*ConcurrentMap)
	memrow, ok := st.lr.Writer().Get(h)
	require.True(t, ok)

	internal := memrow.CollectInternal_(nil)
	require.Len(t, internal, 4)

	for i := 0; i < 4; i++ {
		if i == 2 {
			require.Truef(t, internal[i].SizeMask_()&0x4000_0000 != 0, "entry: %v", internal[i].AllocUnsafe().ToVitess())
		} else {
			require.Truef(t, internal[i].SizeMask_()&0x4000_0000 == 0, "entry: %v", internal[i].AllocUnsafe().ToVitess())
		}
	}

	require.Len(t, memrow.Collect(st.lr.writerVersion.Load(), nil), 4)
	require.Len(t, memrow.Collect(st.lr.writerVersion.Load()+1, nil), 3)

	removedNode := unsafe.Pointer(internal[2])

	w.Swap()

	if offheap.LeakCheck {
		require.True(t, offheap.DefaultAllocator.IsAllocated(removedNode), "pointer for linked list node was freed too early")

		internal = memrow.CollectInternal_(nil)
		require.Len(t, internal, 3)

		for i := 0; i < 3; i++ {
			require.Truef(t, internal[i].SizeMask_()&0x4000_0000 == 0, "entry: %v", internal[i].AllocUnsafe().ToVitess())
		}

		w.Swap()

		require.False(t, offheap.DefaultAllocator.IsAllocated(removedNode), "pointer for linked list should have been freed")
	}
}

func TestUpdateRows(t *testing.T) {
	key := []int{0}
	schema := sql.TestSchema(sqltypes.Int64, sqltypes.Int64)

	r, w := newMapView(testPlan(key), schema, func(iterator []sql.Row) bool {
		return true
	})

	defer func() {
		w.Free()
		offheap.DefaultAllocator.EnsureNoLeaks()
	}()

	row := sql.TestRow(1, 1)
	w.Add([]sql.Record{
		row.ToRecord(true),
	})

	assertLookup(t, r, sql.TestRow(1), 0, false)
	w.Swap()
	assertLookup(t, r, sql.TestRow(1), 1, true)

	w.Add([]sql.Record{row.ToRecord(false), sql.TestRow(1, 2).ToRecord(true)})

	assertLookup(t, r, sql.TestRow(1), 1, true)

	var hasher vthash.Hasher
	h := row.HashWithKey(&hasher, key, schema)
	st := w.store.(*ConcurrentMap)
	memrow, ok := st.lr.Writer().Get(h)
	require.True(t, ok)

	internal := memrow.CollectInternal_(nil)
	require.Len(t, internal, 2)

	w.Swap()

	st.lr.Read(func(tbl offheap.CRowsTable, version uint64) {
		memrow, ok = tbl.Get(h)
		require.True(t, ok)

		internal = memrow.CollectInternal_(nil)
		require.Len(t, internal, 1)
	})

	assertLookup(t, r, sql.TestRow(1), 1, true)
}

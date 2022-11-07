package backlog

import (
	"sync"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/common/rowstore/offheap"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vthash"
)

func TestStoreWorks(t *testing.T) {
	var hasher vthash.Hasher
	a := boostpb.TestRow(1, "a")
	r, w := New([]int{0}, boostpb.TestSchema(sqltypes.Int64, sqltypes.VarChar), nil)

	defer func() {
		w.Free()
		offheap.DefaultAllocator.EnsureNoLeaks()
	}()

	l, hit := Lookup(r, &hasher, a.Slice(0, 1), func(r Rows) int { return r.Len() })
	assert.Equal(t, 0, l)
	assert.True(t, hit)

	w.Add(a.AsRecords(), 0)

	l, hit = Lookup(r, &hasher, a.Slice(0, 1), func(r Rows) int { return r.Len() })
	assert.Equal(t, 0, l)
	assert.True(t, hit)

	w.Swap()

	l, hit = Lookup(r, &hasher, a.Slice(0, 1), func(r Rows) int { return r.Len() })
	assert.Equal(t, 1, l)
	assert.True(t, hit)
}

func TestMinimalQuery(t *testing.T) {
	a := boostpb.TestRow(1, "a")
	b := boostpb.TestRow(1, "b")
	r, w := New([]int{0}, boostpb.TestSchema(sqltypes.Int64, sqltypes.VarChar), nil)

	defer func() {
		w.Free()
		offheap.DefaultAllocator.EnsureNoLeaks()
	}()

	w.Add(a.AsRecords(), 0)
	w.Add(b.AsRecords(), 0)
	w.Swap()

	var hasher vthash.Hasher

	l, hit := Lookup(r, &hasher, a.Slice(0, 1), func(r Rows) int { return r.Len() })
	assert.Equal(t, 2, l)
	assert.True(t, hit)

	Lookup(r, &hasher, a.Slice(0, 1), func(r Rows) int {
		allrows := r.Collect(nil)
		assert.Contains(t, allrows, a)
		assert.Contains(t, allrows, b)
		return r.Len()
	})
}

func TestBusy(t *testing.T) {
	const N = 1000
	var wg sync.WaitGroup

	r, w := New([]int{0}, boostpb.TestSchema(sqltypes.Int64), nil)

	defer func() {
		w.Free()
		offheap.DefaultAllocator.EnsureNoLeaks()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for n := int64(0); n < N; n++ {
			w.Add(boostpb.TestRow(n).AsRecords(), 0)
			w.Swap()
		}
	}()

	var hasher vthash.Hasher
	for n := int64(0); n < N; n++ {
		key := boostpb.RowFromVitess([]sqltypes.Value{sqltypes.NewInt64(n)})
	stress:
		for {
			l, hit := Lookup(r, &hasher, key, func(r Rows) int { return r.Len() })
			switch {
			case !hit:
				continue
			case l == 1:
				break stress
			}
		}
	}

	wg.Wait()
}

func assertLookup(t *testing.T, r *Reader, key boostpb.Row, wantRowsLen int, wantHit bool) {
	t.Helper()

	var hasher vthash.Hasher
	rows, hit := Lookup(r, &hasher, key, func(rows Rows) int {
		return rows.Len()
	})
	require.Equal(t, wantRowsLen, rows)
	require.Equal(t, wantHit, hit)
}

func TestConcurrentRows(t *testing.T) {
	r, w := New([]int{0}, boostpb.TestSchema(sqltypes.Int64, sqltypes.Int64), func(iterator []boostpb.Row) bool {
		return true
	})

	defer func() {
		w.Free()
		offheap.DefaultAllocator.EnsureNoLeaks()
	}()

	w.Add([]boostpb.Record{
		boostpb.TestRow(1, 1).ToRecord(true),
		boostpb.TestRow(1, 2).ToRecord(true),
		boostpb.TestRow(1, 3).ToRecord(true),
		boostpb.TestRow(1, 4).ToRecord(true),
	}, 0)

	assertLookup(t, r, boostpb.TestRow(1), 0, false)
	w.Swap()
	w.Add([]boostpb.Record{boostpb.TestRow(1, 2).ToRecord(false)}, 0)
	assertLookup(t, r, boostpb.TestRow(1), 4, true)
	w.Swap()
	assertLookup(t, r, boostpb.TestRow(1), 3, true)
	w.Add([]boostpb.Record{boostpb.TestRow(1, 1).ToRecord(false)}, 0)
	assertLookup(t, r, boostpb.TestRow(1), 3, true)
	w.Swap()
	assertLookup(t, r, boostpb.TestRow(1), 2, true)
	w.WithKey(boostpb.TestRow(1)).MarkFilled()
	assertLookup(t, r, boostpb.TestRow(1), 2, true)
	w.Swap()
	assertLookup(t, r, boostpb.TestRow(1), 0, true)
}

func TestConcurrentRowsInternal(t *testing.T) {
	key := []int{0}
	schema := boostpb.TestSchema(sqltypes.Int64, sqltypes.Int64)
	r, w := New(key, schema, func(iterator []boostpb.Row) bool {
		return true
	})

	defer func() {
		w.Free()
		offheap.DefaultAllocator.EnsureNoLeaks()
	}()

	w.Add([]boostpb.Record{
		boostpb.TestRow(1, 1).ToRecord(true),
		boostpb.TestRow(1, 2).ToRecord(true),
		boostpb.TestRow(1, 3).ToRecord(true),
		boostpb.TestRow(1, 4).ToRecord(true),
	}, 0)

	assertLookup(t, r, boostpb.TestRow(1), 0, false)
	w.Swap()

	row := boostpb.TestRow(1, 2)
	w.Add([]boostpb.Record{row.ToRecord(false)}, 0)

	var hasher vthash.Hasher
	h := row.HashWithKey(&hasher, key, schema)
	memrow, ok := w.store.tables[(w.store.epoch+1)&0x1].Get(h)
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

	require.Len(t, memrow.Collect(w.store.epoch, nil), 4)
	require.Len(t, memrow.Collect(w.store.epoch+1, nil), 3)

	removedNode := unsafe.Pointer(internal[2])

	w.Swap()

	require.True(t, offheap.DefaultAllocator.IsAllocated(removedNode), "pointer for linked list node was freed too early")

	internal = memrow.CollectInternal_(nil)
	require.Len(t, internal, 3)

	for i := 0; i < 3; i++ {
		require.Truef(t, internal[i].SizeMask_()&0x4000_0000 == 0, "entry: %v", internal[i].AllocUnsafe().ToVitess())
	}

	w.Swap()

	require.False(t, offheap.DefaultAllocator.IsAllocated(removedNode), "pointer for linked list should have been freed")
}

func TestUpdateRows(t *testing.T) {
	key := []int{0}
	schema := boostpb.TestSchema(sqltypes.Int64, sqltypes.Int64)

	r, w := New(key, schema, func(iterator []boostpb.Row) bool {
		return true
	})

	defer func() {
		w.Free()
		offheap.DefaultAllocator.EnsureNoLeaks()
	}()

	row := boostpb.TestRow(1, 1)
	w.Add([]boostpb.Record{
		row.ToRecord(true),
	}, 0)

	assertLookup(t, r, boostpb.TestRow(1), 0, false)
	w.Swap()
	assertLookup(t, r, boostpb.TestRow(1), 1, true)

	w.Add([]boostpb.Record{row.ToRecord(false),
		boostpb.TestRow(1, 2).ToRecord(true)}, 0)

	assertLookup(t, r, boostpb.TestRow(1), 1, true)

	var hasher vthash.Hasher
	h := row.HashWithKey(&hasher, key, schema)
	memrow, ok := w.store.tables[(w.store.epoch+1)&0x1].Get(h)
	require.True(t, ok)

	internal := memrow.CollectInternal_(nil)
	require.Len(t, internal, 2)

	w.Swap()

	memrow, ok = w.store.tables[(w.store.epoch)&0x1].Get(h)
	require.True(t, ok)

	internal = memrow.CollectInternal_(nil)
	require.Len(t, internal, 1)

	assertLookup(t, r, boostpb.TestRow(1), 1, true)
}

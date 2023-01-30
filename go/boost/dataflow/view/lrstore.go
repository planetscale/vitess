package view

import (
	"context"
	"math/rand"
	"runtime"
	"sync/atomic"

	"vitess.io/vitess/go/boost/common/rowstore/offheap"
	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/vt/vthash"
)

type lrstore struct {
	table       leftright
	changelog   Changelog
	writeHasher vthash.Hasher

	waker *waker
}

func newLrstore() *lrstore {
	store := &lrstore{waker: newWaker()}
	store.table.init(runtime.GOMAXPROCS(0))
	return store
}

func (st *lrstore) wClear(key sql.Row, schema []sql.Type, memsize *atomic.Int64) {
	h := key.Hash(&st.writeHasher, schema)
	tbl := st.table.Writer()

	if rows, ok := tbl.Get(h); ok {
		st.changelog.Free(rows)
	}

	tbl.Set(h, nil)
	st.changelog.Do(changeInsert, h, nil)
}

func (st *lrstore) wEmpty(key sql.Row, schema []sql.Type) {
	tbl := st.table.Writer()
	h := key.Hash(&st.writeHasher, schema)

	if free, ok := tbl.Get(h); ok {
		tbl.Remove(h)

		st.changelog.Do(changeRemove, h, nil)
		st.changelog.Free(free)
	}
}

func (st *lrstore) rFound(key sql.Row, schema []sql.Type) (found bool) {
	h := key.Hash(&st.writeHasher, schema)
	st.table.Read(func(tbl offheap.CRowsTable, _ uint64) {
		_, found = tbl.Get(h)
	})
	return
}

func (st *lrstore) wAdd(rs []sql.Record, colLen int, pk []int, schema []sql.Type, memsize *atomic.Int64) {
	tbl := st.table.Writer()
	epoch := st.table.writerVersion.Load() + 1

	for _, r := range rs {
		k := r.Row.HashWithKeySchema(&st.writeHasher, pk, schema)

		rows, ok := tbl.Get(k)
		truncatedRow := r.Row.Truncate(colLen)

		if r.Positive {
			if !ok {
				newrow := offheap.NewConcurrent(truncatedRow, memsize)
				tbl.Set(k, newrow)
				st.changelog.Do(changeInsert, k, newrow)
			} else {
				newrow, free := rows.Insert(truncatedRow, memsize)
				tbl.Set(k, newrow)
				st.changelog.Do(changeInsert, k, newrow)
				st.changelog.Free(free)
			}
		} else {
			if ok {
				tombstoned := rows.Tombstone(truncatedRow, epoch)
				st.changelog.Tombstone(k, tombstoned)
			}
		}
	}
}

func (st *lrstore) wEvict(_ *rand.Rand, bytesToEvict int64) {
	tbl := st.table.Writer()
	tbl.Evict(func(hash vthash.Hash, rows *offheap.ConcurrentRows) bool {
		st.changelog.Do(changeRemove, hash, nil)
		st.changelog.Free(rows)

		bytesToEvict -= rows.TotalMemorySize()
		return bytesToEvict > 0
	})
}

func (st *lrstore) free(memsize *atomic.Int64) {
	st.wRefresh(memsize, true)
	st.table.left.ForEach(func(rows *offheap.ConcurrentRows) {
		rows.Free(memsize)
	})
}

func (st *lrstore) wLen() int {
	return st.table.Writer().Len()
}

func (st *lrstore) rLen() (length int) {
	st.table.Read(func(tbl offheap.CRowsTable, _ uint64) {
		length = tbl.Len()
	})
	return
}

func (st *lrstore) wRefresh(memsize *atomic.Int64, force bool) {
	if !force && st.changelog.IsEmpty() {
		return
	}

	st.table.Publish(func(tbl offheap.CRowsTable) {
		st.changelog.ApplyChanges(tbl, memsize, st.waker)
	})
}

type Rows struct {
	offheap *offheap.ConcurrentRows
	epoch   uint64
}

func (r *Rows) Len() int {
	return r.offheap.Len(r.epoch)
}

func (r *Rows) Collect(rows []sql.Row) []sql.Row {
	return r.offheap.Collect(r.epoch, rows)
}

func Lookup[F func(Rows)](r *Reader, hasher *vthash.Hasher, key sql.Row, then F) (hit bool) {
	hash, exact := key.HashExact(hasher, r.keySchema)
	if !exact {
		then(Rows{})
		return true
	}
	return LookupHash(r, hash, then)
}

func LookupMany[F func(Rows)](r *Reader, hasher *vthash.Hasher, keys []sql.Row, then F) (misses []sql.Row) {
	deduplicate := make(map[vthash.Hash]struct{}, len(keys))

	for _, key := range keys {
		hash, exact := key.HashExact(hasher, r.keySchema)
		if !exact {
			then(Rows{})
			continue
		}
		if _, found := deduplicate[hash]; found {
			continue
		}

		deduplicate[hash] = struct{}{}
		if !LookupHash(r, hash, then) {
			misses = append(misses, key)
		}
	}
	return
}

func LookupHash[F func(Rows)](r *Reader, h vthash.Hash, then F) (hit bool) {
	r.store.table.Read(func(tbl offheap.CRowsTable, version uint64) {
		rows, ok := tbl.Get(h)
		if ok {
			then(Rows{offheap: rows, epoch: version})
			hit = true
		}
		if !ok && r.trigger == nil {
			then(Rows{})
			hit = true
		}
	})
	return
}

func BlockingLookup[F func(Rows)](ctx context.Context, r *Reader, hasher *vthash.Hasher, key sql.Row, then F) (hit bool) {
	h := key.Hash(hasher, r.keySchema)
	_ = r.store.waker.wait(ctx, h, func() bool {
		r.store.table.Read(func(tbl offheap.CRowsTable, version uint64) {
			rows, ok := tbl.Get(h)
			if ok {
				then(Rows{offheap: rows, epoch: version})
				hit = true
			}
		})
		return hit
	})
	return
}

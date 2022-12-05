package backlog

import (
	"context"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/common"
	"vitess.io/vitess/go/boost/common/rowstore/offheap"
	"vitess.io/vitess/go/vt/vthash"
)

type lrstore struct {
	mu     sync.RWMutex
	active [2]int64
	epoch  uintptr

	tables      [2]offheap.CRowsTable
	changelog   Changelog
	writeHasher vthash.Hasher

	waker *waker
}

func newLrstore() *lrstore {
	return &lrstore{
		tables: [2]offheap.CRowsTable{{}, {}},
		waker:  newWaker(),
	}
}

func (st *lrstore) rStart() (offheap.CRowsTable, uintptr) {
	st.mu.RLock()
	lr := st.epoch & 0x1
	atomic.AddInt64(&st.active[lr], 1)
	st.mu.RUnlock()

	return st.tables[lr], lr
}

func (st *lrstore) rFinish(lr uintptr) {
	atomic.AddInt64(&st.active[lr&0x1], -1)
}

func (st *lrstore) wRefresh(memsize *common.AtomicInt64, force bool) {
	if !force && st.changelog.IsEmpty() {
		return
	}

	st.mu.Lock()
	lr := st.epoch & 0x1
	st.epoch++
	st.mu.Unlock()

	for atomic.LoadInt64(&st.active[lr]) > 0 {
		runtime.Gosched()
	}

	writetable := st.tables[lr]
	readtable := st.tables[(lr+1)&0x1]

	st.changelog.ApplyChanges(writetable, readtable, memsize, st.waker)
	// writetable.AssertEquals(readtable)
}

func (st *lrstore) wClear(key boostpb.Row, schema []boostpb.Type, memsize *common.AtomicInt64) {
	h := key.Hash(&st.writeHasher, schema)

	tbl := st.tables[(st.epoch+1)&0x1]

	if rows, ok := tbl.Get(h); ok {
		st.changelog.Free(rows)
	}

	tbl.Set(h, nil)
	st.changelog.Do(changeInsert, h, nil)
}

func (st *lrstore) wEmpty(key boostpb.Row, schema []boostpb.Type) {
	tbl := st.tables[(st.epoch+1)&0x1]
	h := key.Hash(&st.writeHasher, schema)

	if free, ok := tbl.Get(h); ok {
		tbl.Remove(h)

		st.changelog.Do(changeRemove, h, nil)
		st.changelog.Free(free)
	}
}

func (st *lrstore) rFound(key boostpb.Row, schema []boostpb.Type) bool {
	tbl, epoch := st.rStart()
	// it is safe to use the writeHasher here since this method is only called from Domain
	_, found := tbl.Get(key.Hash(&st.writeHasher, schema))
	st.rFinish(epoch)
	return found
}

func (st *lrstore) wAdd(rs []boostpb.Record, colLen int, pk []int, schema []boostpb.Type, memsize *common.AtomicInt64) {
	epoch := st.epoch + 1
	tbl := st.tables[epoch&0x1]

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
				newrow, free := rows.Insert(truncatedRow, epoch, memsize)
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
	epoch := st.epoch + 1
	tbl := st.tables[epoch&0x1]

	tbl.Evict(func(hash vthash.Hash, rows *offheap.ConcurrentRows) bool {
		st.changelog.Do(changeRemove, hash, nil)
		st.changelog.Free(rows)

		bytesToEvict -= rows.TotalMemorySize()
		return bytesToEvict > 0
	})
}

func (st *lrstore) free(memsize *common.AtomicInt64) {
	st.wRefresh(memsize, true)

	st.tables[0].ForEach(func(rows *offheap.ConcurrentRows) {
		rows.Free(memsize)
	})
}

func (st *lrstore) wLen() int {
	return len(st.tables[(st.epoch+1)&0x1])
}

func (st *lrstore) rLen() int {
	tbl, lr := st.rStart()
	l := len(tbl)
	st.rFinish(lr)
	return l
}

type Rows struct {
	offheap *offheap.ConcurrentRows
	epoch   uintptr
}

func (r *Rows) Len() int {
	return r.offheap.Len(r.epoch)
}

func (r *Rows) Collect(rows []boostpb.Row) []boostpb.Row {
	return r.offheap.Collect(r.epoch, rows)
}

func Lookup[F func(Rows)](r *Reader, hasher *vthash.Hasher, key boostpb.Row, then F) (hit bool) {
	hash, exact := key.HashExact(hasher, r.keySchema)
	if !exact {
		then(Rows{})
		return true
	}
	return LookupHash(r, hash, then)
}

func LookupMany[F func(Rows)](r *Reader, hasher *vthash.Hasher, keys []boostpb.Row, then F) (misses []boostpb.Row) {
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
	tbl, epoch := r.store.rStart()
	rows, ok := tbl.Get(h)
	defer r.store.rFinish(epoch)

	if ok {
		then(Rows{offheap: rows, epoch: epoch})
		hit = true
	}
	if !ok && r.trigger == nil {
		then(Rows{})
		hit = true
	}
	return hit
}

func BlockingLookup[F func(Rows)](ctx context.Context, r *Reader, hasher *vthash.Hasher, key boostpb.Row, then F) (hit bool) {
	h := key.Hash(hasher, r.keySchema)
	_ = r.store.waker.wait(ctx, h, func() bool {
		tbl, epoch := r.store.rStart()
		rows, ok := tbl.Get(h)
		defer r.store.rFinish(epoch)

		if ok {
			then(Rows{offheap: rows, epoch: epoch})
			hit = true
			return true
		}
		return false
	})
	return
}

package view

import (
	"math/rand"
	"runtime"
	"sync/atomic"

	"vitess.io/vitess/go/boost/common/rowstore/offheap"
	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/vt/vthash"
)

type ConcurrentMap struct {
	lr          leftright[offheap.CRowsTable]
	changelog   Changelog[vthash.Hash]
	writeHasher vthash.Hasher

	waker *waker
}

func NewConcurrentMap() *ConcurrentMap {
	store := &ConcurrentMap{waker: newWaker()}
	store.lr.init(runtime.GOMAXPROCS(0), make(offheap.CRowsTable), make(offheap.CRowsTable))
	return store
}

func (cm *ConcurrentMap) writerClear(key sql.Row, schema []sql.Type, memsize *atomic.Int64) {
	h := key.Hash(&cm.writeHasher, schema)
	tbl := cm.lr.Writer()

	if rows, ok := tbl.Get(h); ok {
		cm.changelog.Free(rows)
	}

	tbl.Set(h, nil)
	cm.changelog.Do(changeInsert, h, nil)
}

func (cm *ConcurrentMap) writerEmpty(key sql.Row, schema []sql.Type) {
	tbl := cm.lr.Writer()
	h := key.Hash(&cm.writeHasher, schema)

	if free, ok := tbl.Get(h); ok {
		tbl.Remove(h)

		cm.changelog.Do(changeRemove, h, nil)
		cm.changelog.Free(free)
	}
}

func (cm *ConcurrentMap) readerContains(key sql.Row, schema []sql.Type) (found bool) {
	h := key.Hash(&cm.writeHasher, schema)
	cm.lr.Read(func(tbl offheap.CRowsTable, _ uint64) {
		_, found = tbl.Get(h)
	})
	return
}

func (cm *ConcurrentMap) writerAdd(rs []sql.Record, pk []int, schema []sql.Type, memsize *atomic.Int64) {
	tbl := cm.lr.Writer()
	epoch := cm.lr.writerVersion.Load() + 1

	for _, r := range rs {
		k := r.Row.HashWithKeySchema(&cm.writeHasher, pk, schema)
		rows, ok := tbl.Get(k)

		if r.Positive {
			if !ok {
				newrow := offheap.NewConcurrent(r.Row, memsize)
				tbl.Set(k, newrow)
				cm.changelog.Do(changeInsert, k, newrow)
			} else {
				newrow, free := rows.Insert(r.Row, memsize)
				tbl.Set(k, newrow)
				cm.changelog.Do(changeInsert, k, newrow)
				cm.changelog.Free(free)
			}
		} else {
			if ok {
				tombstoned := rows.Tombstone(r.Row, epoch)
				cm.changelog.Tombstone(k, tombstoned)
			}
		}
	}
}

func (cm *ConcurrentMap) writerEvict(_ *rand.Rand, bytesToEvict int64) {
	tbl := cm.lr.Writer()
	tbl.Evict(func(hash vthash.Hash, rows *offheap.ConcurrentRows) bool {
		cm.changelog.Do(changeRemove, hash, nil)
		cm.changelog.Free(rows)

		bytesToEvict -= rows.TotalMemorySize()
		return bytesToEvict > 0
	})
}

func (cm *ConcurrentMap) writerFree(memsize *atomic.Int64) {
	cm.writerRefresh(memsize, true)
	cm.lr.left.ForEach(func(rows *offheap.ConcurrentRows) {
		rows.Free(memsize)
	})
}

func (cm *ConcurrentMap) writerLen() int {
	return cm.lr.Writer().Len()
}

func (cm *ConcurrentMap) readerLen() (length int) {
	cm.lr.Read(func(tbl offheap.CRowsTable, _ uint64) {
		length = tbl.Len()
	})
	return
}

func (cm *ConcurrentMap) writerRefresh(memsize *atomic.Int64, force bool) {
	if !force && cm.changelog.IsEmpty() {
		return
	}

	cm.lr.Publish(func(tbl offheap.CRowsTable) {
		cm.applyChangelog(tbl, memsize)
	})
}

func (cm *ConcurrentMap) applyChangelog(table offheap.CRowsTable, memsize *atomic.Int64) {
	ops := cm.changelog.ops
	diffs := cm.changelog.diffs
	tombstones := cm.changelog.tombstones
	heads := cm.changelog.heads
	freelist := cm.changelog.freelist

	cm.changelog.ops = cm.changelog.ops[:0]
	cm.changelog.diffs = cm.changelog.diffs[:0]
	cm.changelog.tombstones = cm.changelog.tombstones[:0]
	cm.changelog.heads = cm.changelog.heads[:0]
	cm.changelog.freelist = nil

	var wakeup wakeupSet

	for _, head := range heads {
		table.Set(head.key, head.value)
	}

	for i, do := range ops {
		d := diffs[i]
		switch do {
		case changeInsert:
			table.Set(d.key, d.value)
			wakeup.Add(d.key)
		case changeRemove:
			table.Remove(d.key)
		}
	}

	cm.waker.wakeupMany(wakeup)

	for _, ts := range tombstones {
		w, ok := table.Get(ts.key)
		if !ok {
			panic("missing tombstoned entry")
		}
		newhead, free := w.Remove(ts.value)

		if newhead != w {
			table.Set(ts.key, newhead)
			heads = append(heads, diff[vthash.Hash]{ts.key, newhead})
		}

		cm.changelog.Free(free)
	}

	for _, free := range freelist {
		free.Free(memsize)
	}
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

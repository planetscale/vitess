package view

import (
	"math/rand"
	"runtime"
	"sync/atomic"

	"vitess.io/vitess/go/boost/common/rowstore/offheap"
	"vitess.io/vitess/go/boost/dataflow/view/btree"
	"vitess.io/vitess/go/boost/sql"
)

type BTreeMap = btree.Map[sql.Weights, *offheap.ConcurrentRows]

type ConcurrentTree struct {
	lr        leftright[*BTreeMap]
	changelog Changelog[sql.Weights]
}

func NewConcurrentTree() *ConcurrentTree {
	store := &ConcurrentTree{}
	store.lr.init(
		runtime.GOMAXPROCS(0),
		btree.NewMap[sql.Weights, *offheap.ConcurrentRows](0),
		btree.NewMap[sql.Weights, *offheap.ConcurrentRows](0),
	)
	return store
}

func (ct *ConcurrentTree) writerClear(key sql.Row, schema []sql.Type, memsize *atomic.Int64) {
	w, _ := key.Weights(schema)
	tbl := ct.lr.Writer()

	if rows, ok := tbl.Get(w); ok {
		ct.changelog.Free(rows)
	}

	tbl.Set(w, nil)
	ct.changelog.Do(changeInsert, w, nil)
}

func (ct *ConcurrentTree) writerEmpty(key sql.Row, schema []sql.Type) {
	tbl := ct.lr.Writer()
	w, _ := key.Weights(schema)

	if free, ok := tbl.Delete(w); ok {
		ct.changelog.Do(changeRemove, w, nil)
		ct.changelog.Free(free)
	}
}

func (ct *ConcurrentTree) readerContains(key sql.Row, schema []sql.Type) (found bool) {
	w, _ := key.Weights(schema)
	ct.lr.Read(func(tbl *BTreeMap, _ uint64) {
		_, found = tbl.Get(w)
	})
	return
}

func (ct *ConcurrentTree) writerAdd(rs []sql.Record, colLen int, pk []int, schema []sql.Type, memsize *atomic.Int64) {
	tbl := ct.lr.Writer()
	epoch := ct.lr.writerVersion.Load() + 1

	for _, r := range rs {
		w, _ := r.Row.WeightsWithKeySchema(pk, schema, 0)

		rows, ok := tbl.Get(w)
		truncatedRow := r.Row.Truncate(colLen)

		if r.Positive {
			if !ok {
				newrow := offheap.NewConcurrent(truncatedRow, memsize)
				tbl.Set(w, newrow)
				ct.changelog.Do(changeInsert, w, newrow)
			} else {
				newrow, free := rows.Insert(truncatedRow, memsize)
				tbl.Set(w, newrow)
				ct.changelog.Do(changeInsert, w, newrow)
				ct.changelog.Free(free)
			}
		} else {
			if ok {
				tombstoned := rows.Tombstone(truncatedRow, epoch)
				ct.changelog.Tombstone(w, tombstoned)
			}
		}
	}
}

func (ct *ConcurrentTree) writerEvict(_ *rand.Rand, bytesToEvict int64) {
	panic("should never evict from ConcurrentTree")
}

func (ct *ConcurrentTree) writerFree(memsize *atomic.Int64) {
	ct.writerRefresh(memsize, true)
	ct.lr.left.Scan(func(_ sql.Weights, rows *offheap.ConcurrentRows) bool {
		rows.Free(memsize)
		return true
	})
}

func (ct *ConcurrentTree) writerLen() int {
	return ct.lr.Writer().Len()
}

func (ct *ConcurrentTree) readerLen() (length int) {
	ct.lr.Read(func(tbl *BTreeMap, _ uint64) {
		length = tbl.Len()
	})
	return
}

func (ct *ConcurrentTree) writerRefresh(memsize *atomic.Int64, force bool) {
	if !force && ct.changelog.IsEmpty() {
		return
	}

	ct.lr.Publish(func(tbl *BTreeMap) {
		ct.applyChangelog(tbl, memsize)
	})
}

func (ct *ConcurrentTree) applyChangelog(table *BTreeMap, memsize *atomic.Int64) {
	ops := ct.changelog.ops
	diffs := ct.changelog.diffs
	tombstones := ct.changelog.tombstones
	heads := ct.changelog.heads
	freelist := ct.changelog.freelist

	ct.changelog.ops = ct.changelog.ops[:0]
	ct.changelog.diffs = ct.changelog.diffs[:0]
	ct.changelog.tombstones = ct.changelog.tombstones[:0]
	ct.changelog.heads = ct.changelog.heads[:0]
	ct.changelog.freelist = nil

	for _, head := range heads {
		table.Set(head.key, head.value)
	}

	for i, do := range ops {
		d := diffs[i]
		switch do {
		case changeInsert:
			table.Set(d.key, d.value)
		case changeRemove:
			table.Delete(d.key)
		}
	}

	for _, ts := range tombstones {
		w, ok := table.Get(ts.key)
		if !ok {
			panic("missing tombstoned entry")
		}
		newhead, free := w.Remove(ts.value)

		if newhead != w {
			table.Set(ts.key, newhead)
			heads = append(heads, diff[sql.Weights]{ts.key, newhead})
		}

		ct.changelog.Free(free)
	}

	for _, free := range freelist {
		free.Free(memsize)
	}
}

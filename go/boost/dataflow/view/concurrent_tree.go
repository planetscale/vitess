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

type conctree struct {
	b      *BTreeMap
	filled map[sql.Weights]struct{}
}

type ConcurrentTree struct {
	lr        leftright[conctree]
	changelog Changelog[sql.Weights]

	waker *waker
}

func newConctree() conctree {
	return conctree{
		b:      btree.NewMap[sql.Weights, *offheap.ConcurrentRows](0),
		filled: make(map[sql.Weights]struct{}),
	}
}

func NewConcurrentTree() *ConcurrentTree {
	store := &ConcurrentTree{
		waker: newWaker(),
	}
	store.lr.init(runtime.GOMAXPROCS(0), newConctree(), newConctree())
	return store
}

func (ct *ConcurrentTree) writerClear(key sql.Row, schema []sql.Type) {
	w, _ := key.Weights(schema)
	tbl := ct.lr.Writer()

	tbl.filled[w] = struct{}{}
	ct.changelog.Do(changeInsert, w, nil)
}

func (ct *ConcurrentTree) writerEmpty(key sql.Row, schema []sql.Type) {
	tbl := ct.lr.Writer()
	w, _ := key.Weights(schema)

	delete(tbl.filled, w)
	panic("TODO: prefix removal")
}

func (ct *ConcurrentTree) readerContains(key sql.Row, schema []sql.Type) (found bool) {
	w, _ := key.Weights(schema)
	ct.lr.Read(func(tbl conctree, _ uint64) {
		_, found = tbl.filled[w]
	})
	return
}

func (ct *ConcurrentTree) writerAdd(rs []sql.Record, pk []int, schema []sql.Type, memsize *atomic.Int64) {
	tbl := ct.lr.Writer()
	epoch := ct.lr.writerVersion.Load() + 1

	for _, r := range rs {
		w, err := r.Row.WeightsWithKeySchema(pk, schema, 0)
		if err != nil {
			panic(err)
		}
		rows, ok := tbl.b.Get(w)

		if r.Positive {
			if !ok {
				newrow := offheap.NewConcurrent(r.Row, memsize)
				tbl.b.Set(w, newrow)
				ct.changelog.Do(changeInsert, w, newrow)
			} else {
				newrow, free := rows.Insert(r.Row, memsize)
				tbl.b.Set(w, newrow)
				ct.changelog.Do(changeInsert, w, newrow)
				ct.changelog.Free(free)
			}
		} else {
			if ok {
				tombstoned := rows.Tombstone(r.Row, epoch)
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
	ct.lr.left.b.Scan(func(_ sql.Weights, rows *offheap.ConcurrentRows) bool {
		rows.Free(memsize)
		return true
	})
}

func (ct *ConcurrentTree) writerLen() int {
	return ct.lr.Writer().b.Len()
}

func (ct *ConcurrentTree) readerLen() (length int) {
	ct.lr.Read(func(tbl conctree, _ uint64) {
		length = tbl.b.Len()
	})
	return
}

func (ct *ConcurrentTree) writerRefresh(memsize *atomic.Int64, force bool) {
	if !force && ct.changelog.IsEmpty() {
		return
	}

	ct.lr.Publish(func(tbl conctree) {
		ct.applyChangelog(tbl, memsize)
	})
}

func (ct *ConcurrentTree) applyChangelog(ctree conctree, memsize *atomic.Int64) {
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

	var wakeup wakeupSet

	for _, head := range heads {
		ctree.b.Set(head.key, head.value)
	}

	for i, do := range ops {
		d := diffs[i]
		switch do {
		case changeInsert:
			if d.value == nil {
				ctree.filled[d.key] = struct{}{}
				wakeup.AddWeights(d.key)
			} else {
				ctree.b.Set(d.key, d.value)
			}
		case changeRemove:
			delete(ctree.filled, d.key)
			panic("TODO: prefix removal for d.key")
		}
	}

	ct.waker.wakeupMany(wakeup)

	for _, ts := range tombstones {
		w, ok := ctree.b.Get(ts.key)
		if !ok {
			panic("missing tombstoned entry")
		}
		newhead, free := w.Remove(ts.value)

		if newhead != w {
			ctree.b.Set(ts.key, newhead)
			heads = append(heads, diff[sql.Weights]{ts.key, newhead})
		}

		ct.changelog.Free(free)
	}

	for _, free := range freelist {
		free.Free(memsize)
	}
}

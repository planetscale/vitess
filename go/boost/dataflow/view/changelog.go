package view

import (
	"sync/atomic"

	"vitess.io/vitess/go/boost/common/rowstore/offheap"
	"vitess.io/vitess/go/vt/vthash"
)

type diff struct {
	hash  vthash.Hash
	value *offheap.ConcurrentRows
}

type change uint8

const (
	changeInsert change = iota
	changeRemove
)

type Changelog struct {
	ops        []change
	diffs      []diff
	tombstones []diff
	heads      []diff
	freelist   []*offheap.ConcurrentRows
}

func (c *Changelog) IsEmpty() bool {
	return len(c.diffs) == 0 && len(c.freelist) == 0 && len(c.tombstones) == 0
}

func (c *Changelog) Do(ch change, hash vthash.Hash, value *offheap.ConcurrentRows) {
	c.ops = append(c.ops, ch)
	c.diffs = append(c.diffs, diff{hash, value})
}

func (c *Changelog) Tombstone(hash vthash.Hash, value *offheap.ConcurrentRows) {
	c.tombstones = append(c.tombstones, diff{hash, value})
}

func (c *Changelog) Free(r *offheap.ConcurrentRows) {
	if r == nil {
		return
	}
	c.freelist = append(c.freelist, r)
}

func (c *Changelog) ApplyChanges(writetable offheap.CRowsTable, memsize *atomic.Int64, wk *waker) {
	ops := c.ops
	diffs := c.diffs
	tombstones := c.tombstones
	heads := c.heads
	freelist := c.freelist

	c.ops = c.ops[:0]
	c.diffs = c.diffs[:0]
	c.tombstones = c.tombstones[:0]
	c.heads = c.heads[:0]
	c.freelist = nil

	var wakeup wakeupSet

	for _, head := range heads {
		writetable.Set(head.hash, head.value)
	}

	for i, do := range ops {
		d := diffs[i]
		switch do {
		case changeInsert:
			writetable.Set(d.hash, d.value)
			wakeup.Add(d.hash)
		case changeRemove:
			writetable.Remove(d.hash)
		}
	}

	wk.wakeupMany(wakeup)

	for _, ts := range tombstones {
		w, ok := writetable.Get(ts.hash)
		if !ok {
			panic("missing tombstoned entry")
		}
		newhead, free := w.Remove(ts.value)

		if newhead != w {
			writetable.Set(ts.hash, newhead)
			heads = append(heads, diff{ts.hash, newhead})
		}

		c.Free(free)
	}

	for _, free := range freelist {
		free.Free(memsize)
	}
}

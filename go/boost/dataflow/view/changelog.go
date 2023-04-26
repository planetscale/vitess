package view

import "vitess.io/vitess/go/boost/common/rowstore/offheap"

type diff[K any] struct {
	key   K
	value *offheap.ConcurrentRows
}

type change uint8

const (
	changeInsert change = iota
	changeRemove
)

type Changelog[K any] struct {
	ops        []change
	diffs      []diff[K]
	tombstones []diff[K]
	heads      []diff[K]
	freelist   []*offheap.ConcurrentRows
}

func (c *Changelog[K]) IsEmpty() bool {
	return len(c.diffs) == 0 && len(c.freelist) == 0 && len(c.tombstones) == 0
}

func (c *Changelog[K]) Do(ch change, key K, value *offheap.ConcurrentRows) {
	c.ops = append(c.ops, ch)
	c.diffs = append(c.diffs, diff[K]{key, value})
}

func (c *Changelog[K]) Tombstone(key K, value *offheap.ConcurrentRows) {
	c.tombstones = append(c.tombstones, diff[K]{key, value})
}

func (c *Changelog[K]) Free(r *offheap.ConcurrentRows) {
	if r == nil {
		return
	}
	c.freelist = append(c.freelist, r)
}

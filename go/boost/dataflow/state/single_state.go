package state

import (
	"math/rand"
	"sync/atomic"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/common/rowstore/offheap"
	"vitess.io/vitess/go/vt/vthash"
)

type singleState struct {
	key       []int
	keyschema []boostpb.Type
	state     offheap.RowsTable
	partial   bool
	rows      int
	hasher    vthash.Hasher
}

func newSingleState(columns []int, schema []boostpb.Type, partial bool) *singleState {
	var keyschema = make([]boostpb.Type, 0, len(columns))
	for _, col := range columns {
		keyschema = append(keyschema, schema[col])
	}

	return &singleState{
		key:       columns,
		keyschema: keyschema,
		state:     offheap.RowsTable{},
		partial:   partial,
		rows:      0,
	}
}

func (ss *singleState) insertRow(r boostpb.Row, memsize *atomic.Int64) bool {
	key := r.HashWithKeySchema(&ss.hasher, ss.key, ss.keyschema)

	rows, ok := ss.state.Get(key)
	if ok {
		row := rows.Insert(r, memsize)
		ss.state.Set(key, row)
		ss.rows++
		return true
	} else if ss.partial {
		return false
	}

	row := offheap.New(r, memsize)
	ss.state.Set(key, row)
	ss.rows++
	return true
}

func (ss *singleState) removeRow(r boostpb.Row, memsize *atomic.Int64) bool {
	key := r.HashWithKeySchema(&ss.hasher, ss.key, ss.keyschema)
	rows, ok := ss.state.Get(key)
	if !ok {
		return false
	}

	repl, found := rows.Remove(r, memsize)
	ss.state.Set(key, repl)
	if found {
		ss.rows--
	}
	return true
}

func (ss *singleState) lookup(r boostpb.Row) (*offheap.Rows, bool) {
	key := r.Hash(&ss.hasher, ss.keyschema)
	rows, ok := ss.state.Get(key)
	if ok {
		return rows, true
	} else if ss.partial {
		return nil, false
	} else {
		return nil, true
	}
}

func (ss *singleState) markHole(r boostpb.Row, memsize *atomic.Int64) {
	key := r.Hash(&ss.hasher, ss.keyschema)
	old, ok := ss.state.Get(key)
	if !ok {
		panic("tried to mark hole on missing entry")
	}

	old.Free(memsize)
	ss.state.Remove(key)
}

func (ss *singleState) markFilled(r boostpb.Row, memsize *atomic.Int64) {
	key := r.Hash(&ss.hasher, ss.keyschema)
	if rows, ok := ss.state.Get(key); ok {
		rows.Free(memsize)
	}
	ss.state.Set(key, nil)
}

func (ss *singleState) evict(k boostpb.Row, memsize *atomic.Int64) {
	key := k.Hash(&ss.hasher, ss.keyschema)
	if old, ok := ss.state.Get(key); ok {
		old.Free(memsize)
		ss.state.Remove(key)
	}
}

func (ss *singleState) evictKeys(keys []boostpb.Row, memsize *atomic.Int64) {
	for _, k := range keys {
		ss.evict(k, memsize)
	}
}

func (ss *singleState) evictRandomKeys(_ *rand.Rand, target int64, memsize *atomic.Int64) (evicted []boostpb.Row) {
	ss.state.Evict(func(_ vthash.Hash, rows *offheap.Rows) bool {
		rows.ForEach(func(r boostpb.Row) {
			evicted = append(evicted, r.Extract(ss.key))
		})
		rows.Free(memsize)

		// if the current memory size is above our target, keep evicting
		return memsize.Load() > target
	})
	return
}

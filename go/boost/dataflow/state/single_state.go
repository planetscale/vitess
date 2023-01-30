package state

import (
	"math/rand"
	"sync/atomic"

	"vitess.io/vitess/go/boost/common/rowstore/offheap"
	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/vt/vthash"
)

type relatedState struct {
	*singleState
	keymap   []int
	relation stateRelation
}

type stateRelation int

const (
	relationBroader stateRelation = iota
	relationSpecific
	relationNone
)

type singleState struct {
	key       []int
	keyschema []sql.Type
	state     offheap.RowsTable
	hasher    vthash.Hasher
	rows      int
	partial   bool
	primary   bool

	related []relatedState
}

func newSingleState(columns []int, schema []sql.Type, partial, primary bool) *singleState {
	var keyschema = make([]sql.Type, 0, len(columns))
	for _, col := range columns {
		keyschema = append(keyschema, schema[col])
	}

	return &singleState{
		key:       columns,
		keyschema: keyschema,
		state:     offheap.RowsTable{},
		partial:   partial,
		primary:   primary,
		rows:      0,
	}
}

func (ss *singleState) insertRowByPrimaryKey(r sql.Row, memsize *atomic.Int64) {
	key := r.HashWithKeySchema(&ss.hasher, ss.key, ss.keyschema)

	if rows, ok := ss.state.Get(key); ok {
		rows.Free(memsize)
	} else {
		ss.rows++
	}

	row := offheap.New(r, memsize)
	ss.state.Set(key, row)
}

func (ss *singleState) isMissing(r sql.Row) bool {
	key := r.HashWithKeySchema(&ss.hasher, ss.key, ss.keyschema)
	_, ok := ss.state.Get(key)
	return !ok
}

func (ss *singleState) insertRow(r sql.Row, force bool, memsize *atomic.Int64) bool {
	key := r.HashWithKeySchema(&ss.hasher, ss.key, ss.keyschema)

	rows, ok := ss.state.Get(key)
	if ok {
		row := rows.Insert(r, memsize)
		ss.state.Set(key, row)
		ss.rows++
		return true
	} else if ss.partial && !force {
		return false
	}

	row := offheap.New(r, memsize)
	ss.state.Set(key, row)
	ss.rows++
	return true
}

func (ss *singleState) removeRow(r sql.Row, memsize *atomic.Int64) bool {
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

func (ss *singleState) lookup(r sql.Row) (*offheap.Rows, bool) {
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

func (ss *singleState) markHole(r sql.Row, memsize *atomic.Int64) {
	key := r.Hash(&ss.hasher, ss.keyschema)
	old, ok := ss.state.Get(key)
	if !ok {
		panic("tried to mark hole on missing entry")
	}

	old.Free(memsize)
	ss.state.Remove(key)
}

func (ss *singleState) markFilled(r sql.Row, memsize *atomic.Int64) {
	key := r.Hash(&ss.hasher, ss.keyschema)
	if rows, ok := ss.state.Get(key); ok {
		rows.Free(memsize)
	}
	ss.state.Set(key, nil)
}

func (ss *singleState) evict(k sql.Row, memsize *atomic.Int64) {
	key := k.Hash(&ss.hasher, ss.keyschema)
	if old, ok := ss.state.Get(key); ok {
		old.Free(memsize)
		ss.state.Remove(key)
	}
}

func (ss *singleState) evictKeys(keys []sql.Row, memsize *atomic.Int64) {
	for _, k := range keys {
		ss.evict(k, memsize)
	}
}

func (ss *singleState) evictRandomKeys(_ *rand.Rand, target int64, memsize *atomic.Int64) (evicted []sql.Row) {
	ss.state.Evict(func(_ vthash.Hash, rows *offheap.Rows) bool {
		rows.ForEach(func(r sql.Row) {
			evicted = append(evicted, r.Extract(ss.key))
		})
		rows.Free(memsize)

		// if the current memory size is above our target, keep evicting
		return memsize.Load() > target
	})
	return
}

func (ss *singleState) IsEmpty() bool {
	return ss.rows == 0
}

func (ss *singleState) replace(data []sql.Record, memsize *atomic.Int64) {
	if ss.partial {
		panic("unsupported: Replacement on partial state")
	}
	ss.state.Evict(func(_ vthash.Hash, rows *offheap.Rows) bool {
		rows.Free(memsize)
		return true
	})
	for _, r := range data {
		ss.insertRow(r.Row, true, memsize)
	}
}

func (ss *singleState) replaceAndLog(data []sql.Record, memsize *atomic.Int64) (output []sql.Record) {
	if ss.partial {
		panic("unsupported: Replacement on partial state")
	}
	removed := make(map[sql.Row]struct{})
	ss.state.Evict(func(_ vthash.Hash, rows *offheap.Rows) bool {
		rows.ForEach(func(r sql.Row) {
			removed[r] = struct{}{}
		})
		rows.Free(memsize)
		return true
	})

	for _, r := range data {
		ss.insertRow(r.Row, true, memsize)

		if _, rm := removed[r.Row]; rm {
			delete(removed, r.Row)
		} else {
			output = append(output, r)
		}
	}
	for r := range removed {
		output = append(output, r.ToRecord(false))
	}
	return
}

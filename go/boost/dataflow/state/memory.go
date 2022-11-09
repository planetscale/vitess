package state

import (
	"math/rand"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/common"
	"vitess.io/vitess/go/boost/common/rowstore/offheap"
)

type Memory struct {
	state   []*singleState
	byTag   map[boostpb.Tag]int
	memSize common.AtomicInt64
}

func (m *Memory) EvictKeys(tag boostpb.Tag, keys []boostpb.Row) []int {
	if idx, ok := m.byTag[tag]; ok {
		m.state[idx].evictKeys(keys, &m.memSize)
		return m.state[idx].key
	}
	return nil
}

func (m *Memory) stateFor(cols []int) (int, bool) {
	for i, st := range m.state {
		if slices.Equal(st.key, cols) {
			return i, true
		}
	}
	return 0, false
}

func (m *Memory) AddKey(columns []int, schema []boostpb.Type, partial []boostpb.Tag) {
	i, exists := m.stateFor(columns)
	if !exists {
		i = len(m.state)
	}

	for _, tag := range partial {
		m.byTag[tag] = i
	}

	if exists {
		return
	}

	m.state = append(m.state, newSingleState(columns, schema, partial != nil))

	if len(m.state) > 1 && partial == nil {
		newState := m.state[len(m.state)-1]
		m.state[0].state.ForEach(func(rs *offheap.Rows) {
			rs.ForEach(func(r boostpb.Row) {
				newState.insertRow(r, &m.memSize)
			})
		})
	}
}

func (m *Memory) IsUseful() bool {
	return len(m.state) > 0
}

func (m *Memory) IsPartial() bool {
	for _, st := range m.state {
		if st.partial {
			return true
		}
	}
	return false
}

func (m *Memory) ProcessRecords(records *[]boostpb.Record, partialTag boostpb.Tag) {
	if m.IsPartial() {
		rs := (*records)[:0]
		for _, r := range *records {
			var retain bool
			if r.Positive {
				retain = m.insert(r.Row, partialTag)
			} else {
				retain = m.remove(r.Row)
			}
			if retain {
				rs = append(rs, r)
			}
		}
		*records = rs
	} else {
		for _, r := range *records {
			if r.Positive {
				m.insert(r.Row, boostpb.TagNone)
			} else {
				m.remove(r.Row)
			}
		}
	}
}

func (m *Memory) MarkHole(key boostpb.Row, tag boostpb.Tag) {
	index := m.byTag[tag]
	m.state[index].markHole(key, &m.memSize)
}

func (m *Memory) MarkFilled(key boostpb.Row, tag boostpb.Tag) {
	index := m.byTag[tag]
	m.state[index].markFilled(key, &m.memSize)
}

func (m *Memory) Lookup(columns []int, key boostpb.Row) (*offheap.Rows, bool) {
	if len(m.state) == 0 {
		panic("lookup on uninitialized index")
	}
	idx, ok := m.stateFor(columns)
	if !ok {
		panic("lookup on non-indexed column set")
	}
	return m.state[idx].lookup(key)
}

func (m *Memory) Rows() (rows int) {
	for _, ss := range m.state {
		rows += ss.rows
	}
	return
}

func (m *Memory) ClonedState() (state []boostpb.Record) {
	table := m.state[0].state
	state = make([]boostpb.Record, 0, len(table))

	table.ForEach(func(rows *offheap.Rows) {
		rows.ForEach(func(r boostpb.Row) {
			state = append(state, r.ToRecord(true))
		})
	})
	return
}

func (m *Memory) Keys() [][]int {
	//TODO implement me
	panic("implement me")
}

func (m *Memory) ClonedRecords() (records []boostpb.Row) {
	m.state[0].state.ForEach(func(rs *offheap.Rows) {
		records = rs.Collect(records)
	})
	return records
}

func (m *Memory) EvictRandomKeys(rng *rand.Rand, bytesToEvict int64) ([]int, []boostpb.Row) {
	idx := rand.Intn(len(m.state))
	keys := m.state[idx].evictRandomKeys(rng, m.memSize.Load()-bytesToEvict, &m.memSize)
	return m.state[idx].key, keys
}

func (m *Memory) Clear() {
	//TODO implement me
	panic("implement me")
}

func (m *Memory) insert(row boostpb.Row, tag boostpb.Tag) bool {
	if tag != boostpb.TagNone {
		i, ok := m.byTag[tag]
		if !ok {
			// got tagged insert for unknown tag. this will happen if a node on an old
			// replay path is now materialized. must return true to avoid any records
			// (which are destined for a downstream materialization) from being pruned.
			return true
		}
		return m.state[i].insertRow(row, &m.memSize)
	}
	var hit bool
	for _, st := range m.state {
		if st.insertRow(row, &m.memSize) {
			hit = true
		}
	}
	return hit
}

func (m *Memory) remove(row boostpb.Row) bool {
	var hit bool
	for _, s := range m.state {
		hit = hit || s.removeRow(row, &m.memSize)
	}
	return hit
}

func (m *Memory) Free() {
	for _, s := range m.state {
		s.state.ForEach(func(rows *offheap.Rows) {
			rows.Free(&m.memSize)
		})
	}
}

func (m *Memory) StateSizeAtomic() *common.AtomicInt64 {
	return &m.memSize
}

func NewMemoryState() *Memory {
	return &Memory{
		byTag: make(map[boostpb.Tag]int),
	}
}

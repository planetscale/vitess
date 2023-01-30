package state

import (
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"

	"github.com/kr/text"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/dataflow"

	"vitess.io/vitess/go/boost/common/rowstore/offheap"
	"vitess.io/vitess/go/boost/sql"
)

type Map = dataflow.Map[Memory]

type Memory struct {
	state   []*singleState
	byTag   map[dataflow.Tag]int
	memSize atomic.Int64
}

func (m *Memory) GoString() string {
	if len(m.byTag) == 0 {
		return "&Memory{}"
	}

	sorted := maps.Keys(m.byTag)
	slices.Sort(sorted)

	var w strings.Builder
	w.WriteString("&Memory{")
	for i, tag := range sorted {
		if i > 0 {
			w.WriteByte(',')
		}
		ss := m.state[m.byTag[tag]]
		fmt.Fprintf(&w, "\n\t%d (key=%v, primary=%v", tag, ss.key, ss.primary)

		if len(ss.related) > 0 {
			w.WriteString(", rel=")
			for i, rel := range ss.related {
				if i > 0 {
					w.WriteString(", ")
				}
				switch rel.relation {
				case relationBroader:
					fmt.Fprintf(&w, "broader%v", rel.key)
				case relationSpecific:
					fmt.Fprintf(&w, "specific%v", rel.key)
				case relationNone:
					fmt.Fprintf(&w, "none%v", rel.key)
				}
			}
		}

		state := text.Indent(ss.state.GoString(), "\t\t")
		fmt.Fprintf(&w, "):\t%s", strings.TrimSpace(state))
	}
	w.WriteString("\n}")
	return w.String()
}

func (m *Memory) MissingInOtherStates(rs []sql.Record, original dataflow.Tag, each func(tag dataflow.Tag, key []int, r sql.Record)) {
	if len(rs) == 0 {
		return
	}

	for tag, ssIdx := range m.byTag {
		if tag == original {
			continue
		}

		ss := m.state[ssIdx]
		for _, r := range rs {
			if !r.Positive {
				continue
			}
			key := r.Row.Extract(ss.key)
			if _, found := ss.lookup(key); !found {
				each(tag, ss.key, r)
			}
		}
	}
}

func (m *Memory) EvictKeys(tag dataflow.Tag, keys []sql.Row) []int {
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

func (m *Memory) AddKey(columns []int, schema []sql.Type, partial []dataflow.Tag, primary bool) {
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

	us := newSingleState(columns, schema, partial != nil, primary)
	for _, them := range m.state {
		keymapTheirs := overlapKeymap(us.key, them.key)
		keymapOurs := overlapKeymap(them.key, us.key)

		var stateOurs relatedState
		var stateTheirs relatedState

		switch {
		case keymapOurs == nil && keymapTheirs == nil:
			stateOurs = relatedState{relation: relationNone, singleState: them}
			stateTheirs = relatedState{relation: relationNone, singleState: us}
		case keymapOurs == nil && keymapTheirs != nil:
			stateOurs = relatedState{relation: relationSpecific, singleState: them}
			stateTheirs = relatedState{relation: relationBroader, singleState: us, keymap: keymapTheirs}
		case keymapOurs != nil && keymapTheirs == nil:
			stateOurs = relatedState{relation: relationBroader, singleState: them, keymap: keymapOurs}
			stateTheirs = relatedState{relation: relationSpecific, singleState: us}
		default:
			panic("duplicated key in different states")
		}

		us.related = append(us.related, stateOurs)
		them.related = append(them.related, stateTheirs)
	}

	m.state = append(m.state, us)
	if len(m.state) > 1 && partial == nil {
		m.state[0].state.ForEach(func(rs *offheap.Rows) {
			rs.ForEach(func(r sql.Row) {
				us.insertRow(r, true, &m.memSize)
			})
		})
	}
}

func overlapKeymap(short, long []int) []int {
	var keymap []int
	for _, idx := range short {
		i := slices.Index(long, idx)
		if i < 0 {
			return nil
		}
		keymap = append(keymap, i)
	}
	return keymap
}

func (m *Memory) IsUseful() bool {
	return len(m.state) > 0
}

func (m *Memory) IsEmpty() bool {
	for _, st := range m.state {
		if !st.IsEmpty() {
			return false
		}
	}
	return true
}

func (m *Memory) IsPartial() bool {
	for _, st := range m.state {
		if st.partial {
			return true
		}
	}
	return false
}

type RecordMiss func(tag dataflow.Tag, key []int, record sql.Record)

func (m *Memory) ProcessRecords(records []sql.Record, partialTag dataflow.Tag, miss RecordMiss) []sql.Record {
	if m.IsPartial() {
		return sql.FilterRecords(records, func(r sql.Record) bool {
			if r.Positive {
				return m.insert(r, partialTag, miss)
			} else {
				return m.remove(r.Row)
			}
		})
	} else {
		return sql.FilterRecords(records, func(r sql.Record) bool {
			if r.Positive {
				m.insert(r, dataflow.TagNone, nil)
			} else {
				m.remove(r.Row)
			}
			return true
		})
	}
}

func (m *Memory) MarkHole(key sql.Row, tag dataflow.Tag) {
	index := m.byTag[tag]
	m.state[index].markHole(key, &m.memSize)
}

func (m *Memory) MarkFilled(key sql.Row, tag dataflow.Tag) {
	index := m.byTag[tag]
	m.state[index].markFilled(key, &m.memSize)
}

func (m *Memory) Lookup(columns []int, key sql.Row) (*offheap.Rows, bool) {
	if len(m.state) == 0 {
		panic("lookup on uninitialized index")
	}
	idx, ok := m.stateFor(columns)
	if !ok {
		panic("lookup on non-indexed column set")
	}
	ss := m.state[idx]
	if rows, hit := ss.lookup(key); hit {
		return rows, true
	}
	if len(ss.related) == 0 {
		return nil, false
	}

	for _, rel := range ss.related {
		switch rel.relation {
		case relationBroader:
			if rel.keymap == nil {
				panic("broader state without a keymap")
			}
			if _, found := rel.lookup(key.Extract(rel.keymap)); !found {
				return nil, false
			}
		default:
			return nil, false
		}
	}
	return nil, true
}

func (m *Memory) Rows() (rows int) {
	for _, ss := range m.state {
		rows += ss.rows
	}
	return
}

func (m *Memory) ClonedState() (state []sql.Record) {
	if len(m.state) == 0 {
		return nil
	}

	table := m.state[0].state
	state = make([]sql.Record, 0, len(table))

	table.ForEach(func(rows *offheap.Rows) {
		rows.ForEach(func(r sql.Row) {
			state = append(state, r.ToRecord(true))
		})
	})
	return
}

func (m *Memory) ClonedRecords() (records []sql.Row) {
	m.state[0].state.ForEach(func(rs *offheap.Rows) {
		records = rs.Collect(records)
	})
	return records
}

func (m *Memory) EvictRandomKeys(rng *rand.Rand, bytesToEvict int64) ([]int, []sql.Row) {
	idx := rand.Intn(len(m.state))
	keys := m.state[idx].evictRandomKeys(rng, m.memSize.Load()-bytesToEvict, &m.memSize)
	return m.state[idx].key, keys
}

func (m *Memory) insert(r sql.Record, tag dataflow.Tag, miss RecordMiss) bool {
	if tag == dataflow.TagNone {
		var hit bool
		for _, st := range m.state {
			if st.insertRow(r.Row, false, &m.memSize) {
				hit = true
			}
		}
		return hit
	}

	i, ok := m.byTag[tag]
	if !ok {
		// got tagged insert for unknown tag. this will happen if a node on an old
		// replay path is now materialized. must return true to avoid any records
		// (which are destined for a downstream materialization) from being pruned.
		return true
	}
	ss := m.state[i]
	if ss.insertRow(r.Row, false, &m.memSize) {
		for _, rel := range ss.related {
			if rel.primary {
				rel.insertRowByPrimaryKey(r.Row, &m.memSize)
			} else if rel.relation == relationSpecific {
				rel.insertRow(r.Row, true, &m.memSize)
			} else if miss != nil && rel.isMissing(r.Row) {
				miss(tag, rel.key, r)
			}
		}
		return true
	}
	return false
}

func (m *Memory) remove(row sql.Row) bool {
	var hit bool
	for _, s := range m.state {
		if s.removeRow(row, &m.memSize) {
			hit = true
		}
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

func (m *Memory) StateSizeAtomic() *atomic.Int64 {
	return &m.memSize
}

func (m *Memory) Replace(data []sql.Record) []sql.Record {
	replaced := m.state[0].replaceAndLog(data, &m.memSize)
	for _, ss := range m.state[1:] {
		ss.replace(data, &m.memSize)
	}
	return replaced
}

func NewMemoryState() *Memory {
	return &Memory{
		byTag: make(map[dataflow.Tag]int),
	}
}

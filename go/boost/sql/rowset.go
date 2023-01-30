package sql

import (
	"vitess.io/vitess/go/vt/vthash"
)

type RowSet struct {
	set    map[vthash.Hash]Row
	schema []Type
	h      vthash.Hasher
}

func NewRowSet(schema []Type) *RowSet {
	return &RowSet{
		set:    make(map[vthash.Hash]Row),
		schema: schema,
	}
}

func (set *RowSet) Add(row Row) bool {
	hash := row.Hash(&set.h, set.schema)
	_, exists := set.set[hash]
	if !exists {
		set.set[hash] = row
	}
	return !exists
}

func (set *RowSet) Contains(row Row) bool {
	_, ok := set.set[row.Hash(&set.h, set.schema)]
	return ok
}

func (set *RowSet) Remove(row Row) {
	delete(set.set, row.Hash(&set.h, set.schema))
}

func (set *RowSet) ForEach(each func(row Row)) {
	for _, row := range set.set {
		each(row)
	}
}

func (set *RowSet) ToSlice() []Row {
	out := make([]Row, 0, len(set.set))
	for _, r := range set.set {
		out = append(out, r)
	}
	return out
}

func (set *RowSet) Len() int {
	return len(set.set)
}

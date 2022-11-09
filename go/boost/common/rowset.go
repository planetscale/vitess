package common

import (
	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/vt/vthash"
)

type RowSet struct {
	set    map[vthash.Hash]boostpb.Row
	schema []boostpb.Type
	h      vthash.Hasher
}

func NewRowSet(schema []boostpb.Type) RowSet {
	return RowSet{
		set:    make(map[vthash.Hash]boostpb.Row),
		schema: schema,
	}
}

func (set *RowSet) Add(row boostpb.Row) bool {
	hash := row.Hash(&set.h, set.schema)
	_, exists := set.set[hash]
	if !exists {
		set.set[hash] = row
	}
	return !exists
}

func (set *RowSet) Contains(row boostpb.Row) bool {
	_, ok := set.set[row.Hash(&set.h, set.schema)]
	return ok
}

func (set *RowSet) Remove(row boostpb.Row) {
	delete(set.set, row.Hash(&set.h, set.schema))
}

func (set *RowSet) ForEach(each func(row boostpb.Row)) {
	for _, row := range set.set {
		each(row)
	}
}

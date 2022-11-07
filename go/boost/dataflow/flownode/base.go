package flownode

import (
	"bytes"
	"fmt"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/dataflow/state"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vthash"
)

var _ NodeImpl = (*Base)(nil)
var _ AnyBase = (*Base)(nil)

type Base struct {
	primaryKey []int
	schema     []boostpb.Type
	dropped    []int
	unmodified bool
}

func (base *Base) dataflow() {}

func (base *Base) Schema() []boostpb.Type {
	if base.schema == nil {
		panic("no schema initialization")
	}
	return base.schema
}

func (base *Base) Key() ([]int, bool) {
	if base.primaryKey == nil {
		return nil, false
	}
	return base.primaryKey, true
}

func (base *Base) Fix(row boostpb.Row) boostpb.Row {
	if base.unmodified {
		return row
	}
	rowlen := row.Len()
	if rowlen != len(base.schema) {
		build := boostpb.NewRowBuilder(len(base.schema))
		for i := 0; i < rowlen; i++ {
			build.Add(row.ValueAt(i))
		}
		for _, tt := range base.schema[len(row):] {
			build.Add(tt.Default)
		}
		return build.Finish()
	}
	return row
}

func (base *Base) FixRecord(r boostpb.Record) boostpb.Record {
	if base.unmodified {
		return r
	}
	if r.Row.Len() != len(base.schema) {
		panic("unimplemented")
	}
	return r
}

func keyOf(keyCols []int, op *boostpb.TableOperation) boostpb.Row {
	build := boostpb.NewRowBuilder(len(keyCols))
	switch op.Tag {
	case boostpb.TableOperation_Insert:
		for _, col := range keyCols {
			build.Add(op.Key.ValueAt(col))
		}
	case boostpb.TableOperation_Delete:
		for i := range keyCols {
			build.Add(op.Key.ValueAt(i))
		}
	default:
		panic("unsupported")
	}
	return build.Finish()
}

func (base *Base) Process(us boostpb.LocalNodeIndex, ops []*boostpb.TableOperation, state *state.Map) ([]boostpb.Record, error) {
	if base.primaryKey == nil || len(ops) == 0 {
		var records = make([]boostpb.Record, 0, len(ops))
		for _, op := range ops {
			switch op.Tag {
			case boostpb.TableOperation_Insert:
				r := base.Fix(op.Key)
				records = append(records, r.ToRecord(true))
			default:
				return nil, fmt.Errorf("unkeyed base got non-insert operation %v", op)
			}
		}
		return records, nil
	}

	type KeyedOperation struct {
		op   *boostpb.TableOperation
		key  boostpb.Row
		hash vthash.Hash
	}

	var keyedOps []KeyedOperation
	var keyCols = base.primaryKey
	var hasher vthash.Hasher
	for _, op := range ops {
		key := keyOf(keyCols, op)
		hash := key.HashWithKey(&hasher, keyCols, base.schema)
		keyedOps = append(keyedOps, KeyedOperation{op: op, key: key, hash: hash})
	}

	slices.SortFunc(keyedOps, func(a, b KeyedOperation) bool {
		return bytes.Compare(a.hash[:], b.hash[:]) < 0
	})

	thisKey := keyedOps[0].key
	db := state.Get(us)
	if db == nil {
		return nil, fmt.Errorf("base with primary key must be materialized")
	}

	getCurrent := func(currentKey boostpb.Row) boostpb.Row {
		rows, ok := db.Lookup(keyCols, currentKey)
		if !ok {
			panic("miss on base")
		}
		if rows.Len() > 1 {
			panic("primary key was not unique!")
		}
		return rows.First()
	}

	var (
		current = getCurrent(thisKey)
		was     = current
		results []boostpb.Record
	)

	for _, kop := range keyedOps {
		if !boostpb.RowsEqual(thisKey, kop.key) {
			if !boostpb.RowsEqual(current, was) {
				if was != "" {
					results = append(results, was.ToRecord(false))
				}
				if current != "" {
					results = append(results, current.ToRecord(true))
				}
			}

			thisKey = kop.key
			current = getCurrent(thisKey)
			was = current
		}

		switch kop.op.Tag {
		case boostpb.TableOperation_Insert:
			if was != "" {
				// ignoring
			} else {
				current = kop.op.Key
			}
			continue
		case boostpb.TableOperation_Delete:
			current = ""
			continue
		default:
			panic("unsupported")
		}
	}

	if !boostpb.RowsEqual(current, was) {
		if was != "" {
			results = append(results, was.ToRecord(false))
		}
		if current != "" {
			results = append(results, current.ToRecord(true))
		}
	}

	for i, record := range results {
		results[i] = base.FixRecord(record)
	}

	return results, nil
}

func (base *Base) GetDropped() map[int]boostpb.Value {
	var dropped = make(map[int]boostpb.Value)
	for _, col := range base.dropped {
		dropped[col] = base.schema[col].Default
	}
	return dropped
}

func (base *Base) SuggestIndexes(n graph.NodeIdx) map[graph.NodeIdx][]int {
	if base.primaryKey == nil {
		return nil
	}
	return map[graph.NodeIdx][]int{n: base.primaryKey}
}

func NewBase(primaryKey []int, schema []boostpb.Type, defaults []sqltypes.Value) *Base {
	return &Base{
		primaryKey: primaryKey,
		schema:     schema,
		unmodified: true,
	}
}

func (base *Base) ToProto() *boostpb.Node_Base {
	return &boostpb.Node_Base{
		PrimaryKey: base.primaryKey,
		Schema:     base.schema,
		Dropped:    base.dropped,
		Unmodified: base.unmodified,
	}
}

func NewBaseFromProto(base *boostpb.Node_Base) *Base {
	return &Base{
		primaryKey: base.PrimaryKey,
		schema:     base.Schema,
		dropped:    base.Dropped,
		unmodified: base.Unmodified,
	}
}

package view

import (
	"context"
	"math/rand"
	"sync/atomic"

	"vitess.io/vitess/go/boost/common/rowstore/offheap"
	"vitess.io/vitess/go/boost/server/controller/boostplan/viewplan"
	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/vt/vthash"
)

type Writer struct {
	store   Store
	memsize atomic.Int64
	partial bool

	primaryKey []int
	keySchema  []sql.Type
}

type MapReader struct {
	store     *ConcurrentMap
	trigger   func([]sql.Row) bool
	mapKey    []int
	keySchema []sql.Type

	PostFilter *Filter
}

type Store interface {
	readerContains(key sql.Row, schema []sql.Type) (found bool)

	writerLen() int
	writerAdd(rs []sql.Record, pk []int, schema []sql.Type, memsize *atomic.Int64)
	writerEvict(_ *rand.Rand, bytesToEvict int64)
	writerRefresh(memsize *atomic.Int64, force bool)
	writerFree(memsize *atomic.Int64)
	writerEmpty(key sql.Row, schema []sql.Type)
	writerClear(key sql.Row, schema []sql.Type, memsize *atomic.Int64)
}

type Reader interface {
	FullyMaterialized() bool
	Len() int
}

func makeSchema(key []int, schema []sql.Type) (keySchema []sql.Type) {
	keySchema = make([]sql.Type, 0, len(key))
	for _, col := range key {
		keySchema = append(keySchema, schema[col])
	}
	return
}

func isIdentity(key []int) bool {
	for i, k := range key {
		if i != k {
			return false
		}
	}
	return true
}

func NewMapView(key []int, schema []sql.Type, desc *viewplan.Plan, trigger func(iterator []sql.Row) bool) (*MapReader, *Writer) {
	keySchema := makeSchema(key, schema)
	st := NewConcurrentMap()

	r := &MapReader{
		store:     st,
		trigger:   trigger,
		keySchema: keySchema,
	}
	if desc != nil {
		r.PostFilter = newFilter(schema, desc)

		r.mapKey = desc.MapKey
		if len(keySchema) == len(desc.Parameters) && isIdentity(r.mapKey) {
			r.mapKey = nil
		}
	}

	w := &Writer{
		store:      st,
		partial:    trigger != nil,
		primaryKey: key,
		keySchema:  keySchema,
	}

	return r, w
}

func NewTreeView(key []int, schema []sql.Type, desc *viewplan.Plan) (*TreeReader, *Writer) {
	keySchema := makeSchema(key, schema)
	st := NewConcurrentTree()

	r := &TreeReader{
		tree:      st,
		keySchema: keySchema,
	}
	if desc != nil {
		r.PostFilter = newFilter(schema, desc)
		r.treeKey = desc.TreeKey
	}

	w := &Writer{
		store:      st,
		partial:    false,
		primaryKey: key,
		keySchema:  keySchema,
	}

	return r, w
}

func (w *Writer) Add(records []sql.Record) {
	w.store.writerAdd(records, w.primaryKey, w.keySchema, &w.memsize)
}

func (w *Writer) IsPartial() bool {
	return w.partial
}

func (w *Writer) IsEmpty() bool {
	return w.store.writerLen() == 0
}

func (w *Writer) keyFromRecord(row sql.Row) sql.Row {
	var builder = sql.NewRowBuilder(len(w.primaryKey))
	for _, k := range w.primaryKey {
		builder.Add(row.ValueAt(k))
	}
	return builder.Finish()
}

func (w *Writer) WithKey(key sql.Row) *WriteEntry {
	return &WriteEntry{writer: w, key: key}
}

func (w *Writer) EntryFromRecord(row sql.Row) *WriteEntry {
	return w.WithKey(w.keyFromRecord(row))
}

func (w *Writer) Swap() {
	w.store.writerRefresh(&w.memsize, false)
}

func (w *Writer) KeySchema() []sql.Type {
	return w.keySchema
}

func (w *Writer) StateSizeAtomic() *atomic.Int64 {
	return &w.memsize
}

func (w *Writer) EvictRandomKeys(rng *rand.Rand, evict int64) {
	w.store.writerEvict(rng, evict)
}

func (w *Writer) Free() {
	w.store.writerFree(&w.memsize)
}

type WriteEntry struct {
	writer *Writer
	key    sql.Row
}

func (entry *WriteEntry) Found() bool {
	w := entry.writer
	return w.store.readerContains(entry.key, w.keySchema)
}

func (entry *WriteEntry) MarkFilled() {
	w := entry.writer
	w.store.writerClear(entry.key, w.keySchema, &w.memsize)
}

func (entry *WriteEntry) MarkHole() {
	w := entry.writer
	w.store.writerEmpty(entry.key, w.keySchema)
}

func (r *MapReader) Trigger(keys []sql.Row) bool {
	if r.trigger == nil {
		panic("tried to trigger a replay for a fully materialized view")
	}
	if r.mapKey != nil {
		for i, k := range keys {
			keys[i] = k.Extract(r.mapKey)
		}
	}
	return r.trigger(keys)
}

func (r *MapReader) Len() int {
	return r.store.readerLen()
}

func (r *MapReader) FullyMaterialized() bool {
	return r.trigger == nil
}

func (r *MapReader) hash(h *vthash.Hasher, key sql.Row) vthash.Hash {
	if r.mapKey == nil {
		return key.Hash(h, r.keySchema)
	}
	return key.HashWithKeySchema(h, r.mapKey, r.keySchema)
}

func (r *MapReader) Lookup(hasher *vthash.Hasher, key sql.Row, then func(rows Rows)) (hit bool) {
	return r.LookupHash(r.hash(hasher, key), then)
}

func (r *MapReader) LookupMany(hasher *vthash.Hasher, keys []sql.Row, then func(rows Rows)) (misses []sql.Row) {
	deduplicate := make(map[vthash.Hash]struct{}, len(keys))

	for _, key := range keys {
		hash := r.hash(hasher, key)
		if _, found := deduplicate[hash]; found {
			continue
		}

		deduplicate[hash] = struct{}{}
		if !r.LookupHash(hash, then) {
			misses = append(misses, key)
		}
	}
	return
}

func (r *MapReader) LookupHash(h vthash.Hash, then func(rows Rows)) (hit bool) {
	r.store.lr.Read(func(tbl offheap.CRowsTable, version uint64) {
		rows, ok := tbl.Get(h)
		if ok {
			then(Rows{offheap: rows, epoch: version})
			hit = true
		}
		if !ok && r.trigger == nil {
			then(Rows{})
			hit = true
		}
	})
	return
}

func (r *MapReader) BlockingLookup(ctx context.Context, hasher *vthash.Hasher, key sql.Row, then func(rows Rows)) (hit bool) {
	h := r.hash(hasher, key)
	_ = r.store.waker.wait(ctx, h, func() bool {
		r.store.lr.Read(func(tbl offheap.CRowsTable, version uint64) {
			rows, ok := tbl.Get(h)
			if ok {
				then(Rows{offheap: rows, epoch: version})
				hit = true
			}
		})
		return hit
	})
	return
}

type TreeReader struct {
	tree      *ConcurrentTree
	keySchema []sql.Type
	treeKey   *viewplan.Plan_TreeKey

	PostFilter *Filter
}

func (r *TreeReader) Len() int {
	return r.tree.readerLen()
}

func (r *TreeReader) FullyMaterialized() bool {
	// for now Tree views are always fully materialized
	return true
}

type Bound struct {
	weight    sql.Weights
	inclusive bool
}

func (r *TreeReader) iterator(from, to Bound, tree *BTreeMap, version uint64, then func(rows Rows)) {
	switch {
	case from.weight == "":
		switch {
		case to.weight == "":
			tree.Scan(func(_ sql.Weights, rows *offheap.ConcurrentRows) bool {
				then(Rows{rows, version})
				return true
			})
		case to.inclusive:
			tree.Scan(func(key sql.Weights, rows *offheap.ConcurrentRows) bool {
				if key > to.weight {
					return false
				}
				then(Rows{rows, version})
				return true
			})
		default:
			tree.Scan(func(key sql.Weights, rows *offheap.ConcurrentRows) bool {
				if key >= to.weight {
					return false
				}
				then(Rows{rows, version})
				return true
			})
		}
	default:
		switch {
		case to.weight == "":
			tree.Ascend(from.weight, from.inclusive, func(key sql.Weights, rows *offheap.ConcurrentRows) bool {
				then(Rows{rows, version})
				return true
			})
		case to.inclusive:
			tree.Ascend(from.weight, from.inclusive, func(key sql.Weights, rows *offheap.ConcurrentRows) bool {
				if key > to.weight {
					return false
				}
				then(Rows{rows, version})
				return true
			})
		default:
			tree.Ascend(from.weight, from.inclusive, func(key sql.Weights, rows *offheap.ConcurrentRows) bool {
				if key >= to.weight {
					return false
				}
				then(Rows{rows, version})
				return true
			})
		}
	}
}

func (r *TreeReader) Bounds(row sql.Row) (from Bound, to Bound, err error) {
	if r.treeKey.Lower != nil {
		from.inclusive = r.treeKey.LowerInclusive
		from.weight, err = row.WeightsWithKeySchema(r.treeKey.Lower, r.keySchema, 0)
		if err != nil {
			return
		}
	}

	if r.treeKey.Upper != nil {
		to.inclusive = r.treeKey.UpperInclusive
		to.weight, err = row.WeightsWithKeySchema(r.treeKey.Upper, r.keySchema, len(from.weight))
		if err != nil {
			return
		}
	}

	return
}

func (r *TreeReader) LookupRange(from, to Bound, then func(rows Rows)) {
	r.tree.lr.Read(func(tree *BTreeMap, version uint64) {
		r.iterator(from, to, tree, version, then)
	})
}

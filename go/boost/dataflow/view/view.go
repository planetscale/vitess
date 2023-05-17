package view

import (
	"context"
	"math/rand"
	"sync/atomic"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/common/rowstore/offheap"
	"vitess.io/vitess/go/boost/server/controller/boostplan/viewplan"
	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/vt/vthash"
)

type Writer struct {
	store   Store
	memsize atomic.Int64
	partial bool

	internalKey       []int
	externalKey       []int
	internalKeySchema []sql.Type
	externalKeySchema []sql.Type
}

type MapReader struct {
	store        *ConcurrentMap
	trigger      Trigger
	parameterKey []int
	keySchema    []sql.Type

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
	writerClear(key sql.Row, schema []sql.Type)
}

type Reader interface {
	Trigger(keys []sql.Row) bool
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

type Trigger func(iterator []sql.Row) bool

func NewView(desc *viewplan.Plan, schema []sql.Type, trigger Trigger) (Reader, *Writer) {
	if desc.TreeKey != nil {
		return newTreeView(desc, schema, trigger)
	}
	return newMapView(desc, schema, trigger)
}

func newMapView(desc *viewplan.Plan, schema []sql.Type, trigger Trigger) (*MapReader, *Writer) {
	if desc == nil {
		panic("newMapView: no plan description")
	}
	if !slices.Equal(desc.InternalStateKey, desc.ExternalStateKey) {
		panic("newMapView: internal state key is different from external")
	}

	keySchema := makeSchema(desc.ExternalStateKey, schema)
	st := NewConcurrentMap()

	r := &MapReader{
		PostFilter:   newFilter(schema, desc),
		store:        st,
		trigger:      trigger,
		keySchema:    keySchema,
		parameterKey: desc.ParameterKey,
	}

	if len(keySchema) == len(desc.Parameters) && isIdentity(r.parameterKey) {
		r.parameterKey = nil
	}

	w := &Writer{
		store:             st,
		partial:           trigger != nil,
		internalKey:       desc.InternalStateKey,
		externalKey:       desc.ExternalStateKey,
		externalKeySchema: keySchema,
		internalKeySchema: keySchema,
	}

	return r, w
}

func newTreeView(desc *viewplan.Plan, schema []sql.Type, trigger Trigger) (*TreeReader, *Writer) {
	if desc == nil {
		panic("newTreeView: no plan description")
	}

	keySchema := makeSchema(desc.ExternalStateKey, schema)
	st := NewConcurrentTree()

	r := &TreeReader{
		PostFilter:   newFilter(schema, desc),
		tree:         st,
		keySchema:    keySchema,
		trigger:      trigger,
		treeKey:      desc.TreeKey,
		parameterKey: desc.ParameterKey,
	}

	w := &Writer{
		store:             st,
		partial:           trigger != nil,
		internalKey:       desc.InternalStateKey,
		externalKey:       desc.ExternalStateKey,
		externalKeySchema: keySchema,
		internalKeySchema: makeSchema(desc.InternalStateKey, schema),
	}

	return r, w
}

func (w *Writer) Add(records []sql.Record) {
	if len(records) == 0 {
		return
	}
	w.store.writerAdd(records, w.externalKey, w.externalKeySchema, &w.memsize)
}

func (w *Writer) IsPartial() bool {
	return w.partial
}

func (w *Writer) IsEmpty() bool {
	return w.store.writerLen() == 0
}

func (w *Writer) keyFromRecord(row sql.Row) sql.Row {
	var builder = sql.NewRowBuilder(len(w.internalKey))
	for _, k := range w.internalKey {
		builder.Add(row.ValueAt(k))
	}
	return builder.Finish()
}

func (w *Writer) WithKey(key sql.Row) *InternalEntry {
	return &InternalEntry{writer: w, key: key}
}

func (w *Writer) EntryFromRecord(row sql.Row) *InternalEntry {
	return w.WithKey(w.keyFromRecord(row))
}

func (w *Writer) Swap() {
	w.store.writerRefresh(&w.memsize, false)
}

func (w *Writer) KeySchema() []sql.Type {
	return w.internalKeySchema
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

type InternalEntry struct {
	writer *Writer
	key    sql.Row
}

func (entry *InternalEntry) Found() bool {
	w := entry.writer
	return w.store.readerContains(entry.key, w.internalKeySchema)
}

func (entry *InternalEntry) MarkFilled() {
	w := entry.writer
	w.store.writerClear(entry.key, w.internalKeySchema)
}

func (entry *InternalEntry) MarkHole() {
	w := entry.writer
	w.store.writerEmpty(entry.key, w.internalKeySchema)
}

func (r *MapReader) Trigger(keys []sql.Row) bool {
	if r.trigger == nil {
		panic("tried to trigger a replay for a fully materialized view")
	}
	if r.parameterKey != nil {
		for i, k := range keys {
			keys[i] = k.Extract(r.parameterKey)
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
	if r.parameterKey == nil {
		return key.Hash(h, r.keySchema)
	}
	return key.HashWithKeySchema(h, r.parameterKey, r.keySchema)
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
	_ = r.store.waker.waitHash(ctx, h, func() bool {
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
	tree         *ConcurrentTree
	trigger      Trigger
	keySchema    []sql.Type
	parameterKey []int
	treeKey      *viewplan.Plan_TreeKey

	PostFilter *Filter
}

func (r *TreeReader) Len() int {
	return r.tree.readerLen()
}

func (r *TreeReader) FullyMaterialized() bool {
	return r.trigger == nil
}

func (r *TreeReader) Trigger(keys []sql.Row) bool {
	if r.trigger == nil {
		panic("tried to trigger a replay for a fully materialized view")
	}
	if r.parameterKey == nil {
		panic("missing parameterKey in partially materialized view")
	}
	for i, k := range keys {
		keys[i] = k.Extract(r.parameterKey)
	}
	return r.trigger(keys)
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

type Bounds struct {
	Lower  Bound
	Upper  Bound
	Prefix sql.Weights
}

func (r *TreeReader) Bounds(row sql.Row) (bounds Bounds, err error) {
	if r.treeKey.Lower != nil {
		bounds.Lower.inclusive = r.treeKey.LowerInclusive
		bounds.Lower.weight, err = row.WeightsWithKeySchema(r.treeKey.Lower, r.keySchema, 0)
		if err != nil {
			return
		}
	}

	if r.treeKey.Upper != nil {
		bounds.Upper.inclusive = r.treeKey.UpperInclusive
		bounds.Upper.weight, err = row.WeightsWithKeySchema(r.treeKey.Upper, r.keySchema, len(bounds.Lower.weight))
		if err != nil {
			return
		}
	}

	if r.trigger != nil {
		bounds.Prefix, err = row.WeightsWithKeySchema(r.parameterKey, r.keySchema, 0)
		if err != nil {
			return
		}
	}

	return
}

func (r *TreeReader) LookupRange(bounds Bounds, then func(rows Rows)) (hit bool) {
	r.tree.lr.Read(func(tree conctree, version uint64) {
		if r.trigger != nil {
			if _, filled := tree.filled[bounds.Prefix]; !filled {
				return
			}
		}
		r.iterator(bounds.Lower, bounds.Upper, tree.b, version, then)
		hit = true
	})
	return
}

func (r *TreeReader) BlockingLookup(ctx context.Context, bounds Bounds, then func(rows Rows)) (hit bool) {
	_ = r.tree.waker.waitWeights(ctx, bounds.Prefix, func() bool {
		r.tree.lr.Read(func(tree conctree, version uint64) {
			_, hit = tree.filled[bounds.Prefix]
			if hit {
				r.iterator(bounds.Lower, bounds.Upper, tree.b, version, then)
			}
		})
		return hit
	})
	return
}

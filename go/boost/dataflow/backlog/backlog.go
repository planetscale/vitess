package backlog

import (
	"math/rand"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/common"
)

type Writer struct {
	store   *lrstore
	memsize common.AtomicInt64
	partial bool

	primaryKey []int
	keySchema  []boostpb.Type
}

type Reader struct {
	store     *lrstore
	trigger   func([]boostpb.Row) bool
	keySchema []boostpb.Type
}

func New(key []int, schema []boostpb.Type, trigger func(iterator []boostpb.Row) bool) (*Reader, *Writer) {
	var keySchema = make([]boostpb.Type, 0, len(key))
	for _, col := range key {
		keySchema = append(keySchema, schema[col])
	}

	st := newLrstore()

	r := &Reader{
		store:     st,
		trigger:   trigger,
		keySchema: keySchema,
	}
	w := &Writer{
		store:      st,
		partial:    trigger != nil,
		primaryKey: key,
		keySchema:  keySchema,
	}

	return r, w
}

func (r *Reader) Trigger(keys []boostpb.Row) bool {
	if r.trigger == nil {
		panic("tried to trigger a replay for a fully materialized view")
	}
	return r.trigger(keys)
}

func (r *Reader) Len() int {
	return r.store.rLen()
}

func (w *Writer) Add(records []boostpb.Record, colLen int) {
	w.store.wAdd(records, colLen, w.primaryKey, w.keySchema, &w.memsize)
}

func (w *Writer) IsPartial() bool {
	return w.partial
}

func (w *Writer) IsEmpty() bool {
	return w.store.wLen() == 0
}

func (w *Writer) keyFromRecord(row boostpb.Row) boostpb.Row {
	var builder = boostpb.NewRowBuilder(len(w.primaryKey))
	for _, k := range w.primaryKey {
		builder.Add(row.ValueAt(k))
	}
	return builder.Finish()
}

func (w *Writer) WithKey(key boostpb.Row) *WriteEntry {
	return &WriteEntry{writer: w, key: key}
}

func (w *Writer) EntryFromRecord(row boostpb.Row) *WriteEntry {
	return w.WithKey(w.keyFromRecord(row))
}

func (w *Writer) Swap() {
	w.store.wRefresh(&w.memsize, false)
}

func (w *Writer) KeySchema() []boostpb.Type {
	return w.keySchema
}

func (w *Writer) StateSizeAtomic() *common.AtomicInt64 {
	return &w.memsize
}

func (w *Writer) EvictRandomKeys(rng *rand.Rand, evict int64) {
	w.store.wEvict(rng, evict)
}

func (w *Writer) Free() {
	w.store.free(&w.memsize)
}

type WriteEntry struct {
	writer *Writer
	key    boostpb.Row
}

func (entry *WriteEntry) Found() bool {
	w := entry.writer
	return w.store.rFound(entry.key, w.keySchema)
}

func (entry *WriteEntry) MarkFilled() {
	w := entry.writer
	w.store.wClear(entry.key, w.keySchema, &w.memsize)
}

func (entry *WriteEntry) MarkHole() {
	w := entry.writer
	w.store.wEmpty(entry.key, w.keySchema)
}

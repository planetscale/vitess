package view

import (
	"math/rand"
	"sync/atomic"

	"vitess.io/vitess/go/boost/sql"
)

type Writer struct {
	store   *lrstore
	memsize atomic.Int64
	partial bool

	primaryKey []int
	keySchema  []sql.Type
}

type Reader struct {
	store     *lrstore
	trigger   func([]sql.Row) bool
	keySchema []sql.Type
}

func New(key []int, schema []sql.Type, trigger func(iterator []sql.Row) bool) (*Reader, *Writer) {
	var keySchema = make([]sql.Type, 0, len(key))
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

func (r *Reader) Trigger(keys []sql.Row) bool {
	if r.trigger == nil {
		panic("tried to trigger a replay for a fully materialized view")
	}
	return r.trigger(keys)
}

func (r *Reader) Len() int {
	return r.store.rLen()
}

func (w *Writer) Add(records []sql.Record, colLen int) {
	w.store.wAdd(records, colLen, w.primaryKey, w.keySchema, &w.memsize)
}

func (w *Writer) IsPartial() bool {
	return w.partial
}

func (w *Writer) IsEmpty() bool {
	return w.store.wLen() == 0
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
	w.store.wRefresh(&w.memsize, false)
}

func (w *Writer) KeySchema() []sql.Type {
	return w.keySchema
}

func (w *Writer) StateSizeAtomic() *atomic.Int64 {
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
	key    sql.Row
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

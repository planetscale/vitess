package offheap

import (
	"bytes"
	"fmt"
	"strings"
	"unsafe"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/vt/vthash"
)

type CRowsTable map[vthash.Hash]uintptr

func (t CRowsTable) Set(hash vthash.Hash, value *ConcurrentRows) {
	t[hash] = uintptr(unsafe.Pointer(value))
}

func (t CRowsTable) Remove(hash vthash.Hash) {
	delete(t, hash)
}

func (t CRowsTable) Get(hash vthash.Hash) (*ConcurrentRows, bool) {
	ptr, ok := t[hash]
	return (*ConcurrentRows)(unsafe.Pointer(ptr)), ok
}

func (t CRowsTable) ForEach(each func(rows *ConcurrentRows)) {
	for _, r := range t {
		each((*ConcurrentRows)(unsafe.Pointer(r)))
	}
}

func (t CRowsTable) Evict(evict func(h vthash.Hash, rows *ConcurrentRows) bool) {
	for k, r := range t {
		delete(t, k)
		if !evict(k, (*ConcurrentRows)(unsafe.Pointer(r))) {
			return
		}
	}
}

func (t CRowsTable) Len() int {
	return len(t)
}

type RowsTable map[vthash.Hash]uintptr

func (t RowsTable) Set(hash vthash.Hash, value *Rows) {
	t[hash] = uintptr(unsafe.Pointer(value))
}

func (t RowsTable) Remove(hash vthash.Hash) {
	delete(t, hash)
}

func (t RowsTable) Get(hash vthash.Hash) (*Rows, bool) {
	ptr, ok := t[hash]
	return (*Rows)(unsafe.Pointer(ptr)), ok
}

func (t RowsTable) ForEach(each func(*Rows)) {
	for _, r := range t {
		each((*Rows)(unsafe.Pointer(r)))
	}
}

func (t RowsTable) Evict(evict func(vthash.Hash, *Rows) bool) {
	for k, r := range t {
		delete(t, k)
		if !evict(k, (*Rows)(unsafe.Pointer(r))) {
			return
		}
	}
}

func (t RowsTable) GoString() string {
	if len(t) == 0 {
		return "RowsTable{}"
	}

	sorted := maps.Keys(t)
	slices.SortFunc(sorted, func(a, b vthash.Hash) bool {
		return bytes.Compare(a[:], b[:]) < 0
	})

	var w strings.Builder
	w.WriteString("RowsTable{")
	for i, hash := range sorted {
		if i > 0 {
			w.WriteByte(',')
		}
		cr := (*Rows)(unsafe.Pointer(t[hash]))
		fmt.Fprintf(&w, "\n\t%x:\t%v", hash, cr.Collect(nil))
	}
	w.WriteString("\n}")
	return w.String()
}

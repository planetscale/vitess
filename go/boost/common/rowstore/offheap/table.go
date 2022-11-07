package offheap

import (
	"unsafe"

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

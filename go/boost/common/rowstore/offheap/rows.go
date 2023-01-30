package offheap

import (
	"sync/atomic"
	"unsafe"

	"vitess.io/vitess/go/boost/sql"
)

// Rows are used as the linked list for rows in the hash table
// used in the intermediate state where there's only a single
// reader and a single writer. These are not concurrent safe.
//
// The definition here is only the object header. These objects
// are allocated with additional storage to store the actual row.
type Rows struct {
	next *Rows
	size uint32
}

// rowsSize is the size of the object header defined above.
const rowsSize = unsafe.Sizeof(Rows{})

// New allocates a new Rows with the given hash and initial row.
func New(row sql.Row, memsize *atomic.Int64) *Rows {
	hdr := (*Rows)(DefaultAllocator.alloc(rowsSize + uintptr(len(row))))
	hdr.next = nil
	hdr.size = uint32(len(row))

	ary := unsafe.Slice((*byte)(unsafe.Add(unsafe.Pointer(hdr), rowsSize)), len(row))
	copy(ary, row)

	memsize.Add(hdr.memsize())
	return hdr
}

// freeSelf frees only the current node and nothing else in the linked list.
// Should be used for a node that is detached from the linked list.
func (r *Rows) freeSelf(memsize *atomic.Int64) {
	memsize.Add(-r.memsize())
	DefaultAllocator.free(unsafe.Pointer(r))
}

// Free releases the memory allocated for the entire linked list in Rows.
func (r *Rows) Free(memsize *atomic.Int64) {
	free := r
	for free != nil {
		next := free.next
		free.freeSelf(memsize)
		free = next
	}
}

func (r *Rows) memsize() int64 {
	return int64(rowsSize) + int64(r.size)
}

// Insert adds a new element at the header of the linked list
// and returns the new Rows entry created.
func (r *Rows) Insert(row sql.Row, memsize *atomic.Int64) *Rows {
	next := New(row, memsize)
	if r == nil {
		return next
	}

	next.next = r
	return next
}

// Remove drops the given row from the linked list. It returns
// a new linked list as it might be dropping the first element
// of the linked list.
func (r *Rows) Remove(srow sql.Row, memsize *atomic.Int64) (*Rows, bool) {
	var prev *Rows = nil
	var cur = r

	for cur != nil {
		if int(cur.size) == len(srow) && cur.First() == srow {
			goto found
		}
		prev = cur
		cur = cur.next
	}
	return r, false

found:
	if prev == nil {
		if cur.next == nil {
			r.Free(memsize)
			return nil, true
		}

		next := cur.next
		cur.freeSelf(memsize)
		return next, true
	}

	prev.next = cur.next
	cur.freeSelf(memsize)
	return r, true
}

// Len returns the number of items in the linked list.
func (r *Rows) Len() (count int) {
	for ; r != nil; r = r.next {
		count++
	}
	return count
}

// First returns the first entry in the linked list.
// This creates a copy that is safe to use outside
// the context of managed memory.
func (r *Rows) First() sql.Row {
	if r == nil {
		return ""
	}

	// TODO: consider unsafe yield to not allocate
	bytes := unsafe.Slice((*byte)(unsafe.Add(unsafe.Pointer(r), rowsSize)), r.size)
	return sql.Row(bytes)
}

// ForEach yields each row in the linked list. It is
// safe to use this row as it yields a copy of the managed
// memory.
func (r *Rows) ForEach(each func(r sql.Row)) {
	for ; r != nil; r = r.next {
		each(r.First())
	}
}

// Collect returns all rows in the linked list. It is
// safe to use these rows as it returns a copy of the managed
// memory.
func (r *Rows) Collect(rows []sql.Row) []sql.Row {
	for ; r != nil; r = r.next {
		rows = append(rows, r.First())
	}
	return rows
}

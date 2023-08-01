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
const rowsSize = int(unsafe.Sizeof(Rows{}))

func (r *Rows) memsize() int64 {
	return int64(rowsSize) + int64(r.size)
}

// Len returns the number of items in the linked list.
func (r *Rows) Len() (count int) {
	for rows := r; rows != nil; rows = rows.next {
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

// New allocates a new Rows with the given hash and initial row.
func (a *Allocator) New(row sql.Row, memsize *atomic.Int64) *Rows {
	hdr := (*Rows)(a.alloc(rowsSize + len(row)))
	hdr.next = nil
	hdr.size = uint32(len(row))

	ary := unsafe.Slice((*byte)(unsafe.Add(unsafe.Pointer(hdr), rowsSize)), len(row))
	copy(ary, row)

	memsize.Add(hdr.memsize())
	return hdr
}

// Insert adds a new element at the header of the linked list
// and returns the new Rows entry created.
func (a *Allocator) Insert(r *Rows, row sql.Row, memsize *atomic.Int64) *Rows {
	next := a.New(row, memsize)
	if r == nil {
		return next
	}

	next.next = r
	return next
}

// RemoveRows drops the given row from the linked list. It returns
// a new linked list as it might be dropping the first element
// of the linked list.
func (a *Allocator) RemoveRows(r *Rows, srow sql.Row, memsize *atomic.Int64) (*Rows, bool) {
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
			a.FreeRows(r, memsize)
			return nil, true
		}

		next := cur.next
		a.freeSingleRow(cur, memsize)
		return next, true
	}

	prev.next = cur.next
	a.freeSingleRow(cur, memsize)
	return r, true
}

// FreeRows releases the memory allocated for the entire linked list in Rows.
func (a *Allocator) FreeRows(r *Rows, memsize *atomic.Int64) {
	free := r
	for free != nil {
		next := free.next
		a.freeSingleRow(free, memsize)
		free = next
	}
}

// freeSingleRow frees only the current node and nothing else in the linked list.
// Should be used for a node that is detached from the linked list.
func (a *Allocator) freeSingleRow(r *Rows, memsize *atomic.Int64) {
	memsize.Add(-r.memsize())
	a.free(unsafe.Pointer(r))
}

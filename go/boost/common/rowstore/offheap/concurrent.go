package offheap

import (
	"sync/atomic"
	"unsafe"

	"vitess.io/vitess/go/boost/boostpb"
)

// ConcurrentRows are used for the swapping concurrent reading
// safe hash map. It implements a linked list of the rows
// in the map.
// The fields here are the specific header. Additional memory
// is allocated after the initial fields here to store the actual
// row data.
type ConcurrentRows struct {
	next     *ConcurrentRows
	sizemask atomic.Uint32
}

// concurrentRowsSize is the size of the object header defined above.
const concurrentRowsSize = unsafe.Sizeof(ConcurrentRows{})

// flagTombstone is the 30th bit in the sizemask uint32 that indicates if a record
// is tombstoned.
const flagTombstone = 0x4000_0000

// flagEpochPosition is the highest level bit in the sizemask uint32 that indicates
// which of the two sides of the swapping hash table is currently used for writing.
const flagEpochPosition = 31

// NewConcurrent creates a new concurrent row with the given has and row data.
// It allocates the row with the needed size for the row and initializes the
// header correctly.
func NewConcurrent(row boostpb.Row, memsize *atomic.Int64) *ConcurrentRows {
	alloc := concurrentRowsSize + uintptr(len(row))
	hdr := (*ConcurrentRows)(DefaultAllocator.alloc(alloc))
	hdr.next = nil
	hdr.sizemask.Store(uint32(len(row)))

	ary := unsafe.Slice((*byte)(unsafe.Add(unsafe.Pointer(hdr), concurrentRowsSize)), len(row))
	copy(ary, row)

	memsize.Add(int64(alloc))
	return hdr
}

// CollectInternal_ is a helper for tests to iterate over the linked
// list to get all the elements in the hash table row.
func (cr *ConcurrentRows) CollectInternal_(ii []*ConcurrentRows) []*ConcurrentRows {
	for ; cr != nil; cr = cr.next {
		ii = append(ii, cr)
	}
	return ii
}

// ForEachInternal_ is a helper for tests to iterate over the linked
// list with the given callback.
func (cr *ConcurrentRows) ForEachInternal_(each func(rows *ConcurrentRows)) {
	for cr != nil {
		next := cr.next
		each(cr)
		cr = next
	}
}

// SizeMask_ is a helper for tests to get the size mask value
func (cr *ConcurrentRows) SizeMask_() uint32 {
	return cr.sizemask.Load()
}

func (cr *ConcurrentRows) memsize() int64 {
	return int64(concurrentRowsSize) + int64(cr.writesize())
}

// writesize returns the size of the row when treating it as part
// of the write side in the swapping hash table. In case we're writing,
// a tombstone marker should still be seen as valid value and not yet
// as deleted. So we only remove the tombstone mask to get the size.
func (cr *ConcurrentRows) writesize() int {
	return int(cr.sizemask.Load() & (flagTombstone - 1))
}

// readsize returns the size of the row when treating it as part
// of the read side in the swapping hash table. In case we're reading,
// a tombstone marker indicates we should ignore the entry, but only if
// the current epoch matches. Otherwise we still need to see the value
// to guarantee consistencies when deletes are visible.
func (cr *ConcurrentRows) readsize(epoch uintptr) int {
	sz := cr.sizemask.Load()
	if sz&flagTombstone != 0 {
		if sz>>flagEpochPosition == uint32(epoch&0x1) {
			return 0
		}
	}
	return int(sz & (flagTombstone - 1))
}

// Free deallocates the given memory for the concurrent row. If the current
// element in the linked list has the tombstone flag set, we want to only
// delete the current element as we know we're at this point not a member
// of either the write or read side of the hash map.
//
// When no tombstone is set, we delete the entire linked list. This is used
// during cleanup.
func (cr *ConcurrentRows) Free(memsize *atomic.Int64) {
	if cr == nil {
		return
	}

	if cr.sizemask.Load()&flagTombstone != 0 {
		memsize.Add(-cr.memsize())
		DefaultAllocator.free(unsafe.Pointer(cr))
		return
	}

	for cr != nil {
		next := cr.next
		memsize.Add(-cr.memsize())
		DefaultAllocator.free(unsafe.Pointer(cr))
		cr = next
	}
}

func (cr *ConcurrentRows) TotalMemorySize() (total int64) {
	for r := cr; r != nil; r = r.next {
		total += r.memsize()
	}
	return
}

// Insert adds a new row entry to the linked list. It returns
// the new entry which will have the next item set as the current
// element.
// If the current value is the empty sentinel value, we return
// the next element directly. We don't keep the sentinel entry
// at the end of the linked list.
func (cr *ConcurrentRows) Insert(row boostpb.Row, epoch uintptr, memsize *atomic.Int64) (new *ConcurrentRows, free *ConcurrentRows) {
	next := NewConcurrent(row, memsize)
	if cr == nil {
		return next, cr
	}

	next.next = cr
	return next, nil
}

// Tombstone sets the tombstone bit on the current row entry. The row argument
// here is the row we want to mark as tombstone that needs to be part of the
// current linked list.
// We use the current epoch to set the correct epoch bit on the tombstone.
func (cr *ConcurrentRows) Tombstone(row boostpb.Row, epoch uintptr) *ConcurrentRows {
	var cur = cr
	for cur != nil {
		if cur.writesize() == len(row) && cur.AllocUnsafe() == row {
			cur.sizemask.Store(cr.sizemask.Load() | flagTombstone | uint32(epoch&0x1)<<flagEpochPosition)
			return cur
		}
		cur = cur.next
	}
	return nil
}

// Remove drops the given entry from the current linked list. We return the new linked
// list since the given entry can also be the first element of the linked list.
// If we have zero entries left, we return the empty marker so we can store that in
// the hash table.
func (cr *ConcurrentRows) Remove(rowp *ConcurrentRows, memsize *atomic.Int64) (*ConcurrentRows, *ConcurrentRows) {
	var prev *ConcurrentRows = nil
	var cur *ConcurrentRows = cr

	for cur != nil {
		if cur == rowp {
			goto found
		}
		prev = cur
		cur = cur.next
	}
	return cr, nil

found:
	if prev == nil {
		if cur.next == nil {
			return nil, cur
		}
		return cur.next, cur
	}
	prev.next = cur.next
	return cr, cur
}

// Len gets the length of the linked list of rows. It needs
// to walk all the entries.
func (cr *ConcurrentRows) Len(epoch uintptr) (count int) {
	if cr == nil || cr.readsize(epoch) == 0 {
		return 0
	}
	for ; cr != nil; cr = cr.next {
		count++
	}
	return count
}

// AllocUnsafe returns a new ConcurrentRows instance
// that will be backed by the off heap memory. This means it
// is not safe to use this outside of contexts where it's known
// that the memory won't be freed.
func (cr *ConcurrentRows) AllocUnsafe() boostpb.Row {
	bytes := unsafe.Slice((*byte)(unsafe.Add(unsafe.Pointer(cr), rowsSize)), cr.writesize())
	return boostpb.Row(*(*string)(unsafe.Pointer(&bytes)))
}

// alloc returns a new ConcurrentRows instance with the
// given size. This creates a safe copy of the row
// that can be used outside the context of managed memory.
func (cr *ConcurrentRows) alloc(size int) boostpb.Row {
	bytes := unsafe.Slice((*byte)(unsafe.Add(unsafe.Pointer(cr), rowsSize)), size)
	return boostpb.Row(bytes)
}

// ForEach walk the linked list of concurrent rows and calls
// the callback for each entry. It yields an safe copied row that
// can be referenced outside the context of the memory being known
// as allocated.
func (cr *ConcurrentRows) ForEach(epoch uintptr, each func(r boostpb.Row)) {
	for ; cr != nil; cr = cr.next {
		if sz := cr.readsize(epoch); sz != 0 {
			each(cr.alloc(sz))
		}
	}
}

// Collect returns the list of rows stored in this concurrent
// rows entry. The returned entries are safe to retain outside
// the context of memory management, since they are copied
// by this function.
func (cr *ConcurrentRows) Collect(epoch uintptr, rows []boostpb.Row) []boostpb.Row {
	for ; cr != nil; cr = cr.next {
		if sz := cr.readsize(epoch); sz != 0 {
			rows = append(rows, cr.alloc(sz))
		}
	}
	return rows
}

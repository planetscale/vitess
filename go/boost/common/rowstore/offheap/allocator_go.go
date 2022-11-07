//go:build !cgo

package offheap

import (
	"unsafe"

	"vitess.io/vitess/go/vt/log"
)

var DefaultAllocator gomalloc

const LeakCheck = false

// gomalloc is a fallback DefaultAllocator when cgo is disabled at compile time.
// The leak checker is not supported at all when gomalloc is used.
type gomalloc struct{}

// alloc allocates a new piece of memory of the given size.
func (m *gomalloc) alloc(size uintptr) unsafe.Pointer {
	// Complain loudly whenever gomalloc is used. This alloctor should never be
	// used in production code as it defeates the purpose of the offheap
	// allocator.
	log.Warning("cgo disabled, allocating memory using gomalloc")

	b := make([]byte, size)
	return unsafe.Pointer(&b[0])
}

// free frees a given piece of memory.
func (*gomalloc) free(_ unsafe.Pointer) {}

// EnsureNoLeaks can be used in tests to validate no memory has leaked.
func (*gomalloc) EnsureNoLeaks() {}

// IsAllocated checks if a pointer is allocated.
func (*gomalloc) IsAllocated(_ unsafe.Pointer) bool { return false }

//go:build cgo

package offheap

/*
#include <stdlib.h>
*/
import "C"
import (
	"fmt"
	"runtime"
	"sync"
	"unsafe"
)

var DefaultAllocator cmalloc

const LeakCheck = true

// cmalloc tracks each individual allocation for the
// leak checker.
// The lock is used to prevent concurrent access.
type cmalloc struct {
	lock      sync.Mutex
	allocated map[uintptr]*allocation
}

// alloc allocates a new piece of memory of the given size.
// If the leak checker is active, we store the backtrace
// of our caller.
func (m *cmalloc) alloc(size uintptr) unsafe.Pointer {
	ptr := unsafe.Pointer(C.malloc(C.size_t(size)))
	if LeakCheck {
		m.lock.Lock()
		defer m.lock.Unlock()
		if m.allocated == nil {
			m.allocated = make(map[uintptr]*allocation)
		}

		var stack [8]uintptr
		var stacklen = runtime.Callers(1, stack[:])

		m.allocated[uintptr(ptr)] = &allocation{
			alloc: stack[:stacklen],
			free:  nil,
		}
	}
	return ptr
}

// free frees a given piece of memory. If the leak checker
// is active, it validates that the pointer was also allocated
// and that we're not doing a double free.
func (m *cmalloc) free(ptr unsafe.Pointer) {
	if ptr == nil {
		return
	}
	if LeakCheck {
		m.lock.Lock()
		defer m.lock.Unlock()

		alloc, ok := m.allocated[uintptr(ptr)]
		if !ok {
			panic(fmt.Errorf("trying to free non-allocated pointer %p", ptr))
		}
		if alloc.free != nil {
			panic(fmt.Errorf("trying to double-free pointer %p\nALLOC: %s\nFREE: %s", ptr, alloc.AllocStack(), alloc.FreeStack()))
		}

		var stack [8]uintptr
		var stacklen = runtime.Callers(1, stack[:])
		alloc.free = stack[:stacklen]
	}
	C.free(ptr)
}

// EnsureNoLeaks can be used in tests to validate no
// memory has leaked. It depends on the leak checker
// being enabled.
func (m *cmalloc) EnsureNoLeaks() {
	m.lock.Lock()
	defer m.lock.Unlock()

	for ptr, alloc := range m.allocated {
		if alloc.free == nil {
			panic(fmt.Errorf("did not free pointer %#x\nALLOC: %s", ptr, alloc.AllocStack()))
		}
	}
}

// IsAllocated checks if a pointer is allocated. Only
// works if the leak checker is enabled.
func (m *cmalloc) IsAllocated(ptr unsafe.Pointer) bool {
	m.lock.Lock()
	defer m.lock.Unlock()

	alloc, ok := m.allocated[uintptr(ptr)]
	return ok && alloc.free == nil
}

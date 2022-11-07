//go:build !cgo

package offheap

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_gomalloc(t *testing.T) {
	// gomalloc can't do much, just make sure it can allocate memory as a
	// fallback when cgo is disabled. The interface is identical to the cmalloc
	// implementation.
	var m gomalloc
	defer m.EnsureNoLeaks()

	const n = 8

	ptr := m.alloc(n)
	defer m.free(ptr)

	// gomalloc doesn't track allocations by pointer. Just ensure it gave us
	// clean memory to work with.
	require.False(t, m.IsAllocated(ptr))

	buf := (*[n]byte)(ptr)
	for _, b := range buf {
		require.Zero(t, b)
	}
}

//go:build cgo

package offheap

import (
	"testing"
	"unsafe"
)

func Test_cmallocPanics(t *testing.T) {
	if !LeakCheck {
		t.Skip("skipping, LeakCheck is false")
	}

	tests := []struct {
		name string
		op   func(m *cmalloc)
	}{
		{
			name: "leak",
			op: func(m *cmalloc) {
				m.alloc(8)
			},
		},
		{
			name: "free unallocated",
			op: func(m *cmalloc) {
				var i int
				m.free(unsafe.Pointer(&i))
			},
		},
		{
			name: "double free",
			op: func(m *cmalloc) {
				p := m.alloc(8)
				m.free(p)
				m.free(p)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatal("expected panic, but none occurred")
				}

				t.Logf("panic: %v", r)
			}()

			var m cmalloc
			tt.op(&m)
			m.EnsureNoLeaks()
		})
	}
}

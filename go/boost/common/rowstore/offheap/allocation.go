package offheap

import (
	"fmt"
	"runtime"
	"strings"
)

// allocation is used for tracking allocations
// when the leak tracker is enabled.
//
// It keeps track of where the allocation happened
// and where it was freed so we can inform the user
// on both a leak but also on a double free where the
// allocation and the first free happened.
type allocation struct {
	alloc []uintptr
	free  []uintptr
}

// stack renders the given stack trade to a string.
func (a *allocation) stack(ptr []uintptr) string {
	var stack = runtime.CallersFrames(ptr)
	var buf strings.Builder
	for frame, more := stack.Next(); more; frame, more = stack.Next() {
		fmt.Fprintf(&buf, "%s\n\t%s:%d\n", frame.Function, frame.File, frame.Line)
	}
	return buf.String()
}

// AllocStack renders the allocation stack trace.
func (a *allocation) AllocStack() string {
	return a.stack(a.alloc)
}

// FreeStack renders the stack trace for the free.
// Empty if the object wasn't freed yet..
func (a *allocation) FreeStack() string {
	return a.stack(a.free)
}

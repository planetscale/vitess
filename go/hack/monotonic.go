package hack

import (
	"time"
	_ "unsafe"
)

type MonotonicTime int64

func (t MonotonicTime) Elapsed() time.Duration {
	return time.Duration(nanotime() - int64(t))
}

func (t MonotonicTime) After(t2 MonotonicTime) bool {
	return t > t2
}

// Monotonic returns the current time in nanoseconds from a monotonic clock.
func Monotonic() MonotonicTime {
	return MonotonicTime(nanotime())
}

//go:linkname nanotime runtime.nanotime
func nanotime() int64

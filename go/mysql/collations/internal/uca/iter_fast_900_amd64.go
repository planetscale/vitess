package uca

import (
	"math/bits"
	"time"
	"unsafe"
)

var Elapsed time.Duration
var Iters int

func (it *FastIterator900) NextWeightBlock64_asm(dstbytes []byte) int {
	dst := (*[8]uint16)(unsafe.Pointer(&dstbytes[0]))
	p := it.input
	if it.codepoint.ce == 0 && len(p) >= 8 {
		now := time.Now()
		if ucaFastWeight(dst, &p[0], it.fastTable) == 0x88888888 {
			Elapsed += time.Since(now)
			Iters++
			it.input = it.input[8:]
			return 16
		}
	}

	// Slow path: just loop up to 8 times to fill the buffer and bail
	// early if we exhaust the iterator.
	for i := 0; i < 8; i++ {
		w, ok := it.Next()
		if !ok {
			return i * 2
		}
		dst[i] = bits.ReverseBytes16(w)
	}
	return 16
}

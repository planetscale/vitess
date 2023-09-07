//go:build go1.21

package hack

import (
	"math/rand"
	"unsafe"
)

const abiHashTypeHasherOffset = 72

type hashfn func(unsafe.Pointer, uintptr) uintptr

func getRuntimeHasher(hashmap any) hashfn {
	type ipair struct {
		typ unsafe.Pointer
		val unsafe.Pointer
	}

	i := (*ipair)(unsafe.Pointer(&hashmap))
	t := unsafe.Add(i.typ, abiHashTypeHasherOffset)
	return *(*hashfn)(t)
}

//go:nosplit
//go:nocheckptr
func noescape(p unsafe.Pointer) unsafe.Pointer {
	x := uintptr(p)
	//nolint:staticcheck
	return unsafe.Pointer(x ^ 0)
}

type Hasher[K comparable] struct {
	hash hashfn
	seed uintptr
}

func NewHasher[K comparable]() Hasher[K] {
	mapinstance := make(map[K]struct{})
	return Hasher[K]{
		hash: getRuntimeHasher(mapinstance),
		seed: uintptr(rand.Uint64()),
	}
}

func (h Hasher[K]) Hash(key K) uint64 {
	p := noescape(unsafe.Pointer(&key))
	return uint64(h.hash(p, h.seed))
}

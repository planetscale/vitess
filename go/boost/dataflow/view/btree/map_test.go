package btree

import (
	"math/rand"
	"testing"
)

func testPermutation(t *testing.T, integers []int) {
	assert := func(i, x, want, got int, inclusive bool) {
		if want == got {
			return
		}
		for n := i - 3; n < i+3; n++ {
			if n >= 0 && n < len(integers) {
				t.Logf("[%d] %d", n, integers[n])
			}
		}
		t.Fatalf("Ascend(%d, %v) = %d (expected %d)", x, inclusive, got, want)
	}

	tree := NewMap[int, int](0)
	for _, i := range integers {
		tree.Set(i, i)
	}

	for i, x := range integers[:len(integers)-1] {
		tree.Ascend(x, true, func(key int, value int) bool {
			assert(i, x, x, key, true)
			return false
		})
		tree.Ascend(x, false, func(key int, value int) bool {
			assert(i, x, integers[i+1], key, false)
			return false
		})
		tree.Ascend(x+1, true, func(key int, value int) bool {
			assert(i, x, integers[i+1], key, true)
			return false
		})
	}
}

func TestAscendInclusive(t *testing.T) {
	for _, length := range []int{512, 1024, 2048, 4096, 4096 * 2, 4096 * 4, 4096 * 8} {
		var integers []int
		var x int
		for i := 0; i < length; i++ {
			x += 1 + rand.Intn(32)
			integers = append(integers, x)
		}

		testPermutation(t, integers)
	}
}

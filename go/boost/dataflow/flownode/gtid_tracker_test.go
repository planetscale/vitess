package flownode

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vthash"
)

func TestGTIDQueue(t *testing.T) {
	var q gtidQueue

	makehash := func(x byte) vthash.Hash {
		var hash vthash.Hash
		for i := range hash {
			hash[i] = x
		}
		return hash
	}

	for i := 1; i <= 16; i++ {
		hash := makehash(byte(i))
		q.Push(hash)
		time.Sleep(1 * time.Millisecond)
	}

	_, ok := q.Pop(100 * time.Millisecond)
	require.False(t, ok)

	time.Sleep(105 * time.Millisecond)

	for i := 1; i <= 16; i++ {
		h, ok := q.Pop(100 * time.Millisecond)
		require.True(t, ok)
		require.Equal(t, makehash(byte(i)), h)
	}
}

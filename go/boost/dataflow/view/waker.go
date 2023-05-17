package view

import (
	"context"
	"encoding/binary"
	"math/bits"
	"sync"
	"sync/atomic"
	"unsafe"

	"vitess.io/vitess/go/boost/sql"
	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/vt/vthash"
)

type condvar struct {
	mu sync.Mutex
	ch atomic.Pointer[chan struct{}]
}

func hashToHash32(hash vthash.Hash) uint32 {
	return binary.LittleEndian.Uint32(hash[:4])
}

func weightsToHash32(weights sql.Weights) uint32 {
	return uint32(hack.RuntimeStrhash(string(weights), 0x0))
}

func (c *condvar) Init() {
	n := make(chan struct{})
	c.ch.Store(&n)
}

func (c *condvar) Wait(ctx context.Context) error {
	ch := c.NotifyChan()
	c.mu.Unlock()
	select {
	case <-ctx.Done():
	case <-ch:
	}
	c.mu.Lock()
	return ctx.Err()
}

func (c *condvar) NotifyChan() <-chan struct{} {
	return *c.ch.Load()
}

func (c *condvar) Broadcast() {
	n := make(chan struct{})
	close(*c.ch.Swap(&n))
}

const wakerSize = 32
const cacheLine = 128

type waker struct {
	conds [wakerSize]struct {
		condvar
		// padding for false sharing
		pad [cacheLine - unsafe.Sizeof(condvar{})%cacheLine]byte
	}
}

func newWaker() *waker {
	w := &waker{}
	for i := 0; i < wakerSize; i++ {
		w.conds[i].Init()
	}
	return w
}

func (w *waker) wait32(ctx context.Context, hash uint32, try func() bool) error {
	cond := &w.conds[hash%wakerSize]

	cond.mu.Lock()
	defer cond.mu.Unlock()

	for !try() {
		if err := cond.Wait(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (w *waker) waitHash(ctx context.Context, hash vthash.Hash, try func() bool) error {
	return w.wait32(ctx, hashToHash32(hash), try)
}

func (w *waker) waitWeights(ctx context.Context, weights sql.Weights, try func() bool) error {
	return w.wait32(ctx, weightsToHash32(weights), try)
}

func (w *waker) wakeupMany(mask wakeupSet) {
	for mask != 0 {
		cond := &w.conds[bits.TrailingZeros64(uint64(mask))]
		cond.mu.Lock()
		cond.Broadcast()
		cond.mu.Unlock()

		mask ^= mask & -mask
	}
}

type wakeupSet uint64

func (set *wakeupSet) AddHash(hash vthash.Hash) {
	*set = *set | (1 << (hashToHash32(hash) % wakerSize))
}

func (set *wakeupSet) AddWeights(weights sql.Weights) {
	*set = *set | (1 << (weightsToHash32(weights) % wakerSize))
}

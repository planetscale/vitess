package metrics

import (
	"sync"
	"sync/atomic"
	"time"
)

type RateCounter struct {
	period atomic.Uint64

	mu    sync.Mutex
	hits  VariableEWMA
	calls VariableEWMA
}

func NewRateCounter(age time.Duration) *RateCounter {
	return &RateCounter{
		hits:  NewVariableEWMA(age),
		calls: NewVariableEWMA(age),
	}
}

func (r *RateCounter) Register(hit bool) {
	if hit {
		r.period.Add((1 << 32) | 1)
	} else {
		r.period.Add(1)
	}
}

func (r *RateCounter) Tick() {
	period := r.period.Swap(0)
	hits := float64(period >> 32)
	calls := float64(period & 0xFFFFFFFF)

	r.mu.Lock()
	r.hits.Add(hits)
	r.calls.Add(calls)
	r.mu.Unlock()
}

func (r *RateCounter) Rate() float64 {
	var hits, calls float64

	r.mu.Lock()
	hits = r.hits.Value()
	calls = r.calls.Value()
	r.mu.Unlock()

	if calls == 0 {
		return 0
	}
	return hits / calls
}

const WARMUP_SAMPLES = 10

func NewVariableEWMA(averageAge time.Duration) VariableEWMA {
	return VariableEWMA{
		decay: 2 / (averageAge.Seconds() + 1),
		value: 0,
		count: 0,
	}
}

// VariableEWMA represents the exponentially weighted moving average of a series of
// numbers. Unlike SimpleEWMA, it supports a custom age, and thus uses more memory.
type VariableEWMA struct {
	// The multiplier factor by which the previous samples decay.
	decay float64
	// The current value of the average.
	value float64
	// The number of samples added to this instance.
	count uint8
}

// Add adds a value to the series and updates the moving average.
func (e *VariableEWMA) Add(value float64) {
	switch {
	case e.count < WARMUP_SAMPLES:
		e.count++
		e.value += value
	case e.count == WARMUP_SAMPLES:
		e.count++
		e.value = e.value / float64(WARMUP_SAMPLES)
		fallthrough
	default:
		e.value = (value * e.decay) + (e.value * (1 - e.decay))
	}
}

// Value returns the current value of the average, or 0.0 if the series hasn't
// warmed up yet.
func (e *VariableEWMA) Value() float64 {
	if e.count <= WARMUP_SAMPLES {
		return 0.0
	}

	return e.value
}

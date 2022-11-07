package common

import (
	"sync"

	"golang.org/x/exp/maps"
)

type SyncMap[K comparable, V any] struct {
	inner map[K]V
	mu    sync.Mutex
}

func NewSyncMap[K comparable, V any]() *SyncMap[K, V] {
	return &SyncMap[K, V]{inner: make(map[K]V)}
}

func (m *SyncMap[K, V]) Get(k K) (v V, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok = m.inner[k]
	return
}

func (m *SyncMap[K, V]) Set(k K, v V) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inner[k] = v
}

func (m *SyncMap[K, V]) GetOrSet(k K, getv func() (V, error)) (v V, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var ok bool
	v, ok = m.inner[k]
	if !ok {
		v, err = getv()
		if err == nil {
			m.inner[k] = v
		}
	}
	return
}

func (m *SyncMap[K, V]) ForEach(each func(K, V)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range m.inner {
		each(k, v)
	}
}

func (m *SyncMap[K, V]) Delete(k K) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.inner, k)
}

func (m *SyncMap[K, V]) Filter(each func(K, V) bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range m.inner {
		filter := each(k, v)
		if !filter {
			delete(m.inner, k)
		}
	}
}

func (m *SyncMap[K, V]) Replace(r map[K]V) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inner = r
}

func (m *SyncMap[K, V]) Clone() map[K]V {
	m.mu.Lock()
	defer m.mu.Unlock()
	return maps.Clone(m.inner)
}

func (m *SyncMap[K, V]) Take() map[K]V {
	m.mu.Lock()
	defer m.mu.Unlock()

	inner := m.inner
	m.inner = make(map[K]V)
	return inner
}

func (m *SyncMap[K, V]) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for k := range m.inner {
		delete(m.inner, k)
	}
}

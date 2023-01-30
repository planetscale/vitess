package dataflow

import (
	"golang.org/x/exp/slices"
)

type Map[T any] struct {
	n     int
	elems []*T
}

func (s *Map[T]) Insert(idx LocalNodeIdx, v *T) *T {
	addr := int(idx)
	if addr >= len(s.elems) {
		elems := make([]*T, addr+1)
		copy(elems, s.elems)
		s.elems = elems
	}

	old := s.elems[addr]
	s.elems[addr] = v

	if old == nil {
		s.n++
	}
	return old
}

func (s *Map[T]) Get(idx LocalNodeIdx) *T {
	addr := int(idx)
	if addr < len(s.elems) {
		return s.elems[addr]
	}
	return nil
}

func (s *Map[T]) ContainsKey(idx LocalNodeIdx) bool {
	addr := int(idx)
	return addr < len(s.elems) && s.elems[addr] != nil
}

func (s *Map[T]) Remove(idx LocalNodeIdx) *T {
	addr := int(idx)
	if addr >= len(s.elems) {
		return nil
	}
	ret := s.elems[addr]
	if ret != nil {
		s.n--
	}
	return ret
}

func (s *Map[T]) Len() int {
	return s.n
}

func (s *Map[T]) ForEach(each func(index LocalNodeIdx, v *T) bool) {
	for i, n := range s.elems {
		if n == nil {
			continue
		}
		if !each(LocalNodeIdx(i), n) {
			return
		}
	}
}

func (s *Map[T]) Values() (out []*T) {
	for _, n := range s.elems {
		if n != nil {
			out = append(out, n)
		}
	}
	return
}

func (s *Map[T]) Clone() *Map[T] {
	return &Map[T]{
		n:     s.n,
		elems: slices.Clone(s.elems),
	}
}

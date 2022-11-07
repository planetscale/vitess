package state

import (
	"vitess.io/vitess/go/boost/boostpb"
)

type Map struct {
	n     int
	elems []*Memory
}

func (s *Map) Insert(idx boostpb.LocalNodeIndex, st *Memory) *Memory {
	addr := int(idx)
	if addr >= len(s.elems) {
		elems := make([]*Memory, addr+1)
		copy(elems, s.elems)
		s.elems = elems
	}

	old := s.elems[addr]
	s.elems[addr] = st

	if old == nil {
		s.n++
	}
	return old
}

func (s *Map) Get(idx boostpb.LocalNodeIndex) *Memory {
	addr := int(idx)
	if addr < len(s.elems) {
		return s.elems[addr]
	}
	return nil
}

func (s *Map) ContainsKey(idx boostpb.LocalNodeIndex) bool {
	addr := int(idx)
	return addr < len(s.elems) && s.elems[addr] != nil
}

func (s *Map) Remove(idx boostpb.LocalNodeIndex) *Memory {
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

func (s *Map) Len() int {
	return s.n
}

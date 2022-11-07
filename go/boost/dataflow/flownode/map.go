package flownode

import (
	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/boost/boostpb"
)

type Map struct {
	n     int
	elems []*Node
}

func (s *Map) Insert(idx boostpb.LocalNodeIndex, st *Node) *Node {
	addr := int(idx)
	if addr >= len(s.elems) {
		elems := make([]*Node, addr+1)
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

func (s *Map) Get(idx boostpb.LocalNodeIndex) *Node {
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

func (s *Map) Remove(idx boostpb.LocalNodeIndex) *Node {
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

func (s *Map) ForEach(each func(*Node) bool) {
	for _, n := range s.elems {
		if n != nil {
			if !each(n) {
				return
			}
		}
	}
}

func (s *Map) Values() (out []*Node) {
	for _, n := range s.elems {
		if n != nil {
			out = append(out, n)
		}
	}
	return
}

func (s *Map) Clone() *Map {
	return &Map{
		n:     s.n,
		elems: slices.Clone(s.elems),
	}
}

func NewMapFromProto(protomap map[boostpb.LocalNodeIndex]*boostpb.Node) *Map {
	var m Map
	for k, v := range protomap {
		m.Insert(k, NodeFromProto(v))
	}
	return &m
}

func (s *Map) ToProto() map[boostpb.LocalNodeIndex]*boostpb.Node {
	protomap := make(map[boostpb.LocalNodeIndex]*boostpb.Node, len(s.elems))
	for idx, n := range s.elems {
		if n != nil {
			protomap[boostpb.LocalNodeIndex(idx)] = n.ToProto()
		}
	}
	return protomap
}

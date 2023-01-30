package dataflow

import (
	"math"

	"go.uber.org/zap"

	"vitess.io/vitess/go/boost/graph"
)

type LocalNodeIdx uint32

const InvalidLocalNode LocalNodeIdx = math.MaxUint32
const ExternalSource = math.MaxUint32 - 1

type NodeIdx = graph.NodeIdx

func (n LocalNodeIdx) Zap() zap.Field {
	return zap.Uint32("node", uint32(n))
}

func EmptyIndexPair() IndexPair {
	return IndexPair{graph.InvalidNode, InvalidLocalNode}
}

func NewIndexPair(global graph.NodeIdx) IndexPair {
	return IndexPair{Global: global, Local: InvalidLocalNode}
}

func (ip IndexPair) IsEmpty() bool {
	return ip.Global == graph.InvalidNode && ip.Local == InvalidLocalNode
}

func (ip *IndexPair) Remap(remap map[graph.NodeIdx]IndexPair) {
	var ok bool
	*ip, ok = remap[ip.Global]
	if !ok {
		panic("unknown mapping for index")
	}
}

func (ip *IndexPair) SetLocal(local LocalNodeIdx) {
	if ip.Local != InvalidLocalNode {
		panic("trying to double-assign local index")
	}
	ip.Local = local
}

func (ip IndexPair) AsGlobal() graph.NodeIdx {
	return ip.Global
}

func (ip IndexPair) HasLocal() bool {
	return ip.Local != InvalidLocalNode
}

func (ip IndexPair) AsLocal() LocalNodeIdx {
	if ip.Local == InvalidLocalNode {
		panic("unset local index")
	}
	return ip.Local
}

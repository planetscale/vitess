package common

import (
	"sync"
	"sync/atomic"

	"vitess.io/vitess/go/boost/boostpb"
)

type memstatkey struct {
	dom   boostpb.DomainIndex
	shard uint
	node  boostpb.GraphNodeIdx
}

type MemoryStats struct {
	mu    sync.Mutex
	usage map[memstatkey]*atomic.Int64
}

func NewMemStats() *MemoryStats {
	return &MemoryStats{usage: map[memstatkey]*atomic.Int64{}}
}

func (m *MemoryStats) Register(domain boostpb.DomainIndex, shard *uint, node boostpb.GraphNodeIdx, memory *atomic.Int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.usage[memstatkey{dom: domain, shard: UnwrapOr(shard, 0), node: node}] = memory
}

func (m *MemoryStats) ToProto() *boostpb.MemoryStatsResponse {
	m.mu.Lock()
	defer m.mu.Unlock()

	var resp boostpb.MemoryStatsResponse
	for usage, mem := range m.usage {
		resp.NodeUsage = append(resp.NodeUsage, &boostpb.MemoryStatsResponse_MemUsage{
			Domain: boostpb.DomainAddr{
				Domain: usage.dom,
				Shard:  usage.shard,
			},
			Node:   usage.node,
			Memory: mem.Load(),
		})
	}
	return &resp
}

package common

import (
	"sync"

	"go.uber.org/atomic"

	"vitess.io/vitess/go/boost/boostpb"
)

type memstatkey struct {
	dom   boostpb.DomainIndex
	shard uint
	node  boostpb.GraphNodeIdx
}

type MemoryStats struct {
	mu    sync.Mutex
	usage map[memstatkey]*AtomicInt64
}

func NewMemStats() *MemoryStats {
	return &MemoryStats{usage: map[memstatkey]*AtomicInt64{}}
}

func (m *MemoryStats) Register(domain boostpb.DomainIndex, shard *uint, node boostpb.GraphNodeIdx, memory *AtomicInt64) {
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

// AtomicInt64 is an atomic Int64 type
// TODO: switch to the standard libary's builtin type when we upgrade to Go 1.19
type AtomicInt64 = atomic.Int64

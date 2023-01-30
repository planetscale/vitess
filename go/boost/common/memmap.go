package common

import (
	"sync"
	"sync/atomic"

	"vitess.io/vitess/go/boost/boostrpc/service"
	"vitess.io/vitess/go/boost/dataflow"
)

type memstatkey struct {
	dom   dataflow.DomainIdx
	shard uint
	node  dataflow.NodeIdx
}

type MemoryStats struct {
	mu    sync.Mutex
	usage map[memstatkey]*atomic.Int64
}

func NewMemStats() *MemoryStats {
	return &MemoryStats{usage: map[memstatkey]*atomic.Int64{}}
}

func (m *MemoryStats) Register(domain dataflow.DomainIdx, shard *uint, node dataflow.NodeIdx, memory *atomic.Int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.usage[memstatkey{dom: domain, shard: UnwrapOr(shard, 0), node: node}] = memory
}

func (m *MemoryStats) ToProto() *service.MemoryStatsResponse {
	m.mu.Lock()
	defer m.mu.Unlock()

	var resp service.MemoryStatsResponse
	for usage, mem := range m.usage {
		resp.NodeUsage = append(resp.NodeUsage, &service.MemoryStatsResponse_MemUsage{
			Domain: dataflow.DomainAddr{
				Domain: usage.dom,
				Shard:  usage.shard,
			},
			Node:   usage.node,
			Memory: mem.Load(),
		})
	}
	return &resp
}

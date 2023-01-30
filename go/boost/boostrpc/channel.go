package boostrpc

import (
	"fmt"

	"vitess.io/vitess/go/boost/common"
	"vitess.io/vitess/go/boost/dataflow"
)

type ChannelCoordinator struct {
	locals *common.SyncMap[dataflow.DomainAddr, DomainClient]
	remote *common.SyncMap[dataflow.DomainAddr, string]
}

func NewChannelCoordinator() *ChannelCoordinator {
	return &ChannelCoordinator{
		locals: common.NewSyncMap[dataflow.DomainAddr, DomainClient](),
		remote: common.NewSyncMap[dataflow.DomainAddr, string](),
	}
}

func (coord *ChannelCoordinator) InsertLocal(dom dataflow.DomainIdx, shard uint, domobj DomainClient) {
	coord.locals.Set(dataflow.DomainAddr{Domain: dom, Shard: shard}, domobj)
}

func (coord *ChannelCoordinator) InsertRemote(dom dataflow.DomainIdx, shard uint, addr string) {
	coord.remote.Set(dataflow.DomainAddr{Domain: dom, Shard: shard}, addr)
}

func (coord *ChannelCoordinator) GetClient(dom dataflow.DomainIdx, shard uint) (DomainClient, error) {
	domaddr := dataflow.DomainAddr{Domain: dom, Shard: shard}
	if local, ok := coord.locals.Get(domaddr); ok {
		return local, nil
	}
	if addr, ok := coord.remote.Get(domaddr); ok {
		return NewRemoteDomainClient(dom, shard, addr)
	}
	return nil, fmt.Errorf("unknown shard: %d:%d", dom, shard)
}

func (coord *ChannelCoordinator) GetAddr(dom dataflow.DomainIdx, shard uint) string {
	addr, _ := coord.remote.Get(dataflow.DomainAddr{Domain: dom, Shard: shard})
	return addr
}

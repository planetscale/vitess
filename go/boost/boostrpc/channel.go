package boostrpc

import (
	"fmt"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/common"
)

type ChannelCoordinator struct {
	locals *common.SyncMap[boostpb.DomainAddr, DomainClient]
	remote *common.SyncMap[boostpb.DomainAddr, string]
}

func NewChannelCoordinator() *ChannelCoordinator {
	return &ChannelCoordinator{
		locals: common.NewSyncMap[boostpb.DomainAddr, DomainClient](),
		remote: common.NewSyncMap[boostpb.DomainAddr, string](),
	}
}

func (coord *ChannelCoordinator) InsertLocal(dom boostpb.DomainIndex, shard uint, domobj DomainClient) {
	coord.locals.Set(boostpb.DomainAddr{Domain: dom, Shard: shard}, domobj)
}

func (coord *ChannelCoordinator) InsertRemote(dom boostpb.DomainIndex, shard uint, addr string) {
	coord.remote.Set(boostpb.DomainAddr{Domain: dom, Shard: shard}, addr)
}

func (coord *ChannelCoordinator) GetClient(dom boostpb.DomainIndex, shard uint) (DomainClient, error) {
	domaddr := boostpb.DomainAddr{Domain: dom, Shard: shard}
	if local, ok := coord.locals.Get(domaddr); ok {
		return local, nil
	}
	if addr, ok := coord.remote.Get(domaddr); ok {
		return NewRemoteDomainClient(dom, shard, addr)
	}
	return nil, fmt.Errorf("unknown shard: %d:%d", dom, shard)
}

func (coord *ChannelCoordinator) GetAddr(dom boostpb.DomainIndex, shard uint) string {
	addr, _ := coord.remote.Get(boostpb.DomainAddr{Domain: dom, Shard: shard})
	return addr
}

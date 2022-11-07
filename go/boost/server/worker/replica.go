package worker

import (
	"context"
	"fmt"

	"storj.io/drpc/drpcmetadata"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/boostrpc"
	"vitess.io/vitess/go/boost/common"
	"vitess.io/vitess/go/boost/dataflow/domain"
)

type Replica struct {
	boostpb.DRPCInnerDomainUnimplementedServer

	sender *DomainSenders
	domain *domain.Domain
	target string
}

func NewReplica(d *domain.Domain, coord *boostrpc.ChannelCoordinator) *Replica {
	replica := &Replica{
		domain: d,
		sender: newDomainSenders(coord),
		target: fmt.Sprintf("%d.%d", d.Index(), common.UnwrapOr(d.Shard(), 0)),
	}

	return replica
}

func (r *Replica) Reactor(ctx context.Context) { r.domain.Reactor(ctx, r.sender) }

func newDomainSenders(coord *boostrpc.ChannelCoordinator) *DomainSenders {
	return &DomainSenders{
		coord: coord,
		conns: common.NewSyncMap[boostpb.DomainAddr, boostrpc.DomainClient](),
	}
}

type DomainSenders struct {
	coord *boostrpc.ChannelCoordinator
	conns *common.SyncMap[boostpb.DomainAddr, boostrpc.DomainClient]
}

func (o *DomainSenders) Send(ctx context.Context, dest boostpb.DomainAddr, m *boostpb.Packet) error {
	client, err := o.conns.GetOrSet(dest, func() (boostrpc.DomainClient, error) {
		return o.coord.GetClient(dest.Domain, dest.Shard)
	})
	if err != nil {
		return err
	}
	return client.ProcessAsync(ctx, m)
}

func (r *Replica) ProcessAsync(ctx context.Context, packet *boostpb.Packet) (*boostpb.PacketResponse, error) {
	if metadata, ok := drpcmetadata.Get(ctx); ok {
		if target, ok := metadata["target_domain"]; ok {
			if target != r.target {
				return nil, fmt.Errorf("packet for %s was received at %s", target, r.target)
			}
		}
	}
	_ = r.domain.ProcessAsync(ctx, packet)
	return &boostpb.PacketResponse{}, nil
}

func (r *Replica) ProcessSync(ctx context.Context, packet *boostpb.SyncPacket) (*boostpb.PacketResponse, error) {
	if err := r.domain.ProcessSync(ctx, packet); err != nil {
		return nil, err
	}
	return &boostpb.PacketResponse{}, nil
}

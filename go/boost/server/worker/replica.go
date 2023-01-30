package worker

import (
	"context"
	"fmt"

	"storj.io/drpc/drpcmetadata"

	"vitess.io/vitess/go/boost/boostrpc"
	"vitess.io/vitess/go/boost/boostrpc/packet"
	"vitess.io/vitess/go/boost/common"
	"vitess.io/vitess/go/boost/dataflow"
	"vitess.io/vitess/go/boost/dataflow/domain"
)

type Replica struct {
	packet.DRPCDomainUnimplementedServer

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
		conns: common.NewSyncMap[dataflow.DomainAddr, boostrpc.DomainClient](),
	}
}

type DomainSenders struct {
	coord *boostrpc.ChannelCoordinator
	conns *common.SyncMap[dataflow.DomainAddr, boostrpc.DomainClient]
}

func (o *DomainSenders) Send(ctx context.Context, dest dataflow.DomainAddr, pkt packet.FlowPacket) error {
	client, err := o.conns.GetOrSet(dest, func() (boostrpc.DomainClient, error) {
		return o.coord.GetClient(dest.Domain, dest.Shard)
	})
	if err != nil {
		return err
	}
	switch pkt := pkt.(type) {
	case *packet.Message:
		_, err = client.SendMessage(ctx, pkt)
	case *packet.ReplayPiece:
		_, err = client.SendReplayPiece(ctx, pkt)
	case *packet.EvictKeysRequest:
		_, err = client.EvictKeys(ctx, pkt)
	default:
		err = fmt.Errorf("unexpected packet in processing.Sender: %T", pkt)
	}
	return err
}

func (r *Replica) check(ctx context.Context) error {
	if metadata, ok := drpcmetadata.Get(ctx); ok {
		if target, ok := metadata["target_domain"]; ok {
			if target != r.target {
				return fmt.Errorf("packet for %s was received at %s", target, r.target)
			}
		}
	}
	return nil
}

func (r *Replica) Close() {}

func (r *Replica) SendInput(ctx context.Context, in *packet.Input) (*packet.Async, error) {
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	err := r.domain.ProcessAsync(ctx, in)
	return &packet.Async{}, err
}

func (r *Replica) SendMessage(ctx context.Context, in *packet.Message) (*packet.Async, error) {
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	err := r.domain.ProcessAsync(ctx, in)
	return &packet.Async{}, err
}

func (r *Replica) SendReplayPiece(ctx context.Context, in *packet.ReplayPiece) (*packet.Async, error) {
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	err := r.domain.ProcessAsync(ctx, in)
	return &packet.Async{}, err
}

func (r *Replica) Evict(ctx context.Context, in *packet.EvictRequest) (*packet.Async, error) {
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	err := r.domain.ProcessAsync(ctx, in)
	return &packet.Async{}, err
}

func (r *Replica) EvictKeys(ctx context.Context, in *packet.EvictKeysRequest) (*packet.Async, error) {
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	err := r.domain.ProcessAsync(ctx, in)
	return &packet.Async{}, err
}

func (r *Replica) StartPartialReplay(ctx context.Context, in *packet.PartialReplayRequest) (*packet.Async, error) {
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	err := r.domain.ProcessAsync(ctx, in)
	return &packet.Async{}, err
}

func (r *Replica) StartReaderReplay(ctx context.Context, in *packet.ReaderReplayRequest) (*packet.Async, error) {
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	err := r.domain.ProcessAsync(ctx, in)
	return &packet.Async{}, err
}

func (r *Replica) StartReplay(ctx context.Context, in *packet.StartReplayRequest) (*packet.Async, error) {
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	err := r.domain.ProcessAsync(ctx, in)
	return &packet.Async{}, err
}

func (r *Replica) AddNode(ctx context.Context, in *packet.AddNodeRequest) (*packet.Async, error) {
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	err := r.domain.ProcessAsync(ctx, in)
	return &packet.Async{}, err
}

func (r *Replica) FinishReplay(ctx context.Context, in *packet.FinishReplayRequest) (*packet.Async, error) {
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	err := r.domain.ProcessAsync(ctx, in)
	return &packet.Async{}, err
}

func (r *Replica) RemoveNodes(ctx context.Context, in *packet.RemoveNodesRequest) (*packet.Async, error) {
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	err := r.domain.ProcessAsync(ctx, in)
	return &packet.Async{}, err
}

func (r *Replica) UpdateEgress(ctx context.Context, in *packet.UpdateEgressRequest) (*packet.Async, error) {
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	err := r.domain.ProcessAsync(ctx, in)
	return &packet.Async{}, err
}

func (r *Replica) UpdateSharder(ctx context.Context, in *packet.UpdateSharderRequest) (*packet.Async, error) {
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	err := r.domain.ProcessAsync(ctx, in)
	return &packet.Async{}, err
}

func (r *Replica) PrepareState(ctx context.Context, in *packet.PrepareStateRequest) (*packet.Async, error) {
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	err := r.domain.ProcessAsync(ctx, in)
	return &packet.Async{}, err
}

func (r *Replica) SetupReplayPath(ctx context.Context, in *packet.SetupReplayPathRequest) (*packet.Sync, error) {
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	err := r.domain.ProcessSync(ctx, in)
	return &packet.Sync{}, err
}

func (r *Replica) Ready(ctx context.Context, in *packet.ReadyRequest) (*packet.Sync, error) {
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	err := r.domain.ProcessSync(ctx, in)
	return &packet.Sync{}, err
}

func (r *Replica) WaitForReplay(ctx context.Context, _ *packet.WaitForReplayRequest) (*packet.Sync, error) {
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	err := r.domain.WaitForReplay(ctx)
	return &packet.Sync{}, err
}

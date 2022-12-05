package domainrpc

import (
	"context"
	"fmt"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/boostrpc"
)

type ShardHandle struct {
	worker WorkerID
	client boostrpc.DomainClient
}

func NewShardHandle(id WorkerID, client boostrpc.DomainClient) ShardHandle {
	return ShardHandle{
		worker: id,
		client: client,
	}
}

type Handle struct {
	idx    boostpb.DomainIndex
	shards []ShardHandle
}

func NewHandle(idx boostpb.DomainIndex, shards []ShardHandle) *Handle {
	return &Handle{
		idx:    idx,
		shards: shards,
	}
}

func (h *Handle) Shards() uint {
	return uint(len(h.shards))
}

func (h *Handle) Assignment(shard uint) WorkerID {
	return h.shards[shard].worker
}

func (h *Handle) SendToHealthy(ctx context.Context, p *boostpb.Packet, workers map[WorkerID]*Worker) error {
	for _, shrd := range h.shards {
		if workers[shrd.worker].Healthy {
			if err := shrd.client.ProcessAsync(ctx, p); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("worker %s is not currently healthy", shrd.worker)
		}
	}
	return nil
}

func (h *Handle) SendToHealthySync(ctx context.Context, p *boostpb.SyncPacket, workers map[WorkerID]*Worker) error {
	for _, shrd := range h.shards {
		if workers[shrd.worker].Healthy {
			if err := shrd.client.ProcessSync(ctx, p); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("worker %s is not currently healthy", shrd.worker)
		}
	}
	return nil
}

func (h *Handle) SendToHealthyShard(ctx context.Context, i uint, p *boostpb.Packet, workers map[WorkerID]*Worker) error {
	if workers[h.shards[i].worker].Healthy {
		return h.shards[i].client.ProcessAsync(ctx, p)
	}
	return fmt.Errorf("tried to send packet to failed worker; ignoring")
}

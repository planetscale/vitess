package controller

import (
	"context"
	"fmt"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/boostrpc"
)

type DomainShardHandle struct {
	worker WorkerID
	client boostrpc.DomainClient
}

type DomainHandle struct {
	idx    boostpb.DomainIndex
	shards []DomainShardHandle
}

func (h *DomainHandle) Shards() uint {
	return uint(len(h.shards))
}

func (h *DomainHandle) Assignment(shard uint) WorkerID {
	return h.shards[shard].worker
}

func (h *DomainHandle) SendToHealthy(ctx context.Context, p *boostpb.Packet, workers map[WorkerID]*Worker) error {
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

func (h *DomainHandle) SendToHealthySync(ctx context.Context, p *boostpb.SyncPacket, workers map[WorkerID]*Worker) error {
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

func (h *DomainHandle) SendToHealthyShard(ctx context.Context, i uint, p *boostpb.Packet, workers map[WorkerID]*Worker) error {
	if workers[h.shards[i].worker].Healthy {
		return h.shards[i].client.ProcessAsync(ctx, p)
	}
	return fmt.Errorf("tried to send packet to failed worker; ignoring")
}

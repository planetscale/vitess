package domainrpc

import (
	"context"
	"fmt"

	"go.uber.org/multierr"

	"vitess.io/vitess/go/boost/boostrpc/packet"
	"vitess.io/vitess/go/boost/dataflow"

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
	idx    dataflow.DomainIdx
	shards []ShardHandle
}

func NewHandle(idx dataflow.DomainIdx, shards []ShardHandle) *Handle {
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

type Client interface {
	Ready(request *packet.ReadyRequest) error
	PrepareState(request *packet.PrepareStateRequest) error
	StartReplay(request *packet.StartReplayRequest) error
	WaitForReplay() error
	SetupReplayPath(request *packet.SetupReplayPathRequest) error
	UpdateEgress(request *packet.UpdateEgressRequest) error
	AddNode(request *packet.AddNodeRequest) error
	RemoveNodes(request *packet.RemoveNodesRequest) error
	UpdateSharder(request *packet.UpdateSharderRequest) error
}

func (h *Handle) Client(ctx context.Context, workers map[WorkerID]*Worker) Client {
	return &client{
		ctx:     ctx,
		shards:  h.shards,
		workers: workers,
	}
}

func (h *Handle) ShardClient(ctx context.Context, i uint, workers map[WorkerID]*Worker) Client {
	return &client{
		ctx:     ctx,
		shards:  []ShardHandle{h.shards[i]},
		workers: workers,
	}
}

type client struct {
	ctx     context.Context
	shards  []ShardHandle
	workers map[WorkerID]*Worker
}

func (c *client) Ready(request *packet.ReadyRequest) error {
	return c.send(func(d boostrpc.DomainClient) error {
		_, err := d.Ready(c.ctx, request)
		return err
	})
}

func (c *client) PrepareState(request *packet.PrepareStateRequest) error {
	return c.send(func(d boostrpc.DomainClient) error {
		_, err := d.PrepareState(c.ctx, request)
		return err
	})
}

func (c *client) StartReplay(request *packet.StartReplayRequest) error {
	return c.send(func(d boostrpc.DomainClient) error {
		_, err := d.StartReplay(c.ctx, request)
		return err
	})
}

func (c *client) WaitForReplay() error {
	return c.send(func(d boostrpc.DomainClient) error {
		_, err := d.WaitForReplay(c.ctx, &packet.WaitForReplayRequest{})
		return err
	})
}

func (c *client) SetupReplayPath(request *packet.SetupReplayPathRequest) error {
	return c.send(func(d boostrpc.DomainClient) error {
		_, err := d.SetupReplayPath(c.ctx, request)
		return err
	})
}

func (c *client) UpdateEgress(request *packet.UpdateEgressRequest) error {
	return c.send(func(d boostrpc.DomainClient) error {
		_, err := d.UpdateEgress(c.ctx, request)
		return err
	})
}

func (c *client) UpdateSharder(request *packet.UpdateSharderRequest) error {
	return c.send(func(d boostrpc.DomainClient) error {
		_, err := d.UpdateSharder(c.ctx, request)
		return err
	})
}

func (c *client) AddNode(request *packet.AddNodeRequest) error {
	return c.send(func(d boostrpc.DomainClient) error {
		_, err := d.AddNode(c.ctx, request)
		return err
	})
}

func (c *client) RemoveNodes(request *packet.RemoveNodesRequest) error {
	return c.send(func(d boostrpc.DomainClient) error {
		_, err := d.RemoveNodes(c.ctx, request)
		return err
	})
}

func (c *client) send(call func(d boostrpc.DomainClient) error) error {
	var errs []error
	for _, shrd := range c.shards {
		var err error
		if c.workers[shrd.worker].Healthy {
			err = call(shrd.client)
		} else {
			err = fmt.Errorf("worker %s is not currently healthy", shrd.worker)
		}
		if err != nil {
			errs = append(errs, err)
		}
	}
	return multierr.Combine(errs...)
}

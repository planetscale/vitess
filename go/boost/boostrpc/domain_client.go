package boostrpc

import (
	"context"
	"fmt"
	"net"
	"sync"

	"storj.io/drpc/drpcconn"
	"storj.io/drpc/drpcmetadata"

	"vitess.io/vitess/go/boost/boostrpc/packet"
	"vitess.io/vitess/go/boost/dataflow"
)

type DomainClient interface {
	packet.DRPCDomainServer
	Close()
}

var _ DomainClient = (*RemoteDomainClient)(nil)

type RemoteDomainClient struct {
	mu           sync.Mutex
	inner        packet.DRPCDomainClient
	targetDomain dataflow.DomainIdx
	targetShard  uint
}

func (d *RemoteDomainClient) SendInput(ctx context.Context, in *packet.Input) (*packet.Async, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.inner.SendInput(d.context(ctx), in)
}

func (d *RemoteDomainClient) SendMessage(ctx context.Context, in *packet.Message) (*packet.Async, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.inner.SendMessage(d.context(ctx), in)
}

func (d *RemoteDomainClient) SendReplayPiece(ctx context.Context, in *packet.ReplayPiece) (*packet.Async, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.inner.SendReplayPiece(d.context(ctx), in)
}

func (d *RemoteDomainClient) Evict(ctx context.Context, in *packet.EvictRequest) (*packet.Async, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.inner.Evict(d.context(ctx), in)
}

func (d *RemoteDomainClient) EvictKeys(ctx context.Context, in *packet.EvictKeysRequest) (*packet.Async, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.inner.EvictKeys(d.context(ctx), in)
}

func (d *RemoteDomainClient) StartPartialReplay(ctx context.Context, in *packet.PartialReplayRequest) (*packet.Async, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.inner.StartPartialReplay(d.context(ctx), in)
}

func (d *RemoteDomainClient) StartReaderReplay(ctx context.Context, in *packet.ReaderReplayRequest) (*packet.Async, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.inner.StartReaderReplay(d.context(ctx), in)
}

func (d *RemoteDomainClient) StartReplay(ctx context.Context, in *packet.StartReplayRequest) (*packet.Async, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.inner.StartReplay(d.context(ctx), in)
}

func (d *RemoteDomainClient) AddNode(ctx context.Context, in *packet.AddNodeRequest) (*packet.Async, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.inner.AddNode(d.context(ctx), in)
}

func (d *RemoteDomainClient) FinishReplay(ctx context.Context, in *packet.FinishReplayRequest) (*packet.Async, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.inner.FinishReplay(d.context(ctx), in)
}

func (d *RemoteDomainClient) RemoveNodes(ctx context.Context, in *packet.RemoveNodesRequest) (*packet.Async, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.inner.RemoveNodes(d.context(ctx), in)
}

func (d *RemoteDomainClient) UpdateEgress(ctx context.Context, in *packet.UpdateEgressRequest) (*packet.Async, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.inner.UpdateEgress(d.context(ctx), in)
}

func (d *RemoteDomainClient) UpdateSharder(ctx context.Context, in *packet.UpdateSharderRequest) (*packet.Async, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.inner.UpdateSharder(d.context(ctx), in)
}

func (d *RemoteDomainClient) PrepareState(ctx context.Context, in *packet.PrepareStateRequest) (*packet.Async, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.inner.PrepareState(d.context(ctx), in)
}

func (d *RemoteDomainClient) context(ctx context.Context) context.Context {
	return drpcmetadata.Add(ctx, "target_domain", fmt.Sprintf("%d.%d", d.targetDomain, d.targetShard))
}

func (d *RemoteDomainClient) SetupReplayPath(ctx context.Context, in *packet.SetupReplayPathRequest) (*packet.Sync, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.inner.SetupReplayPath(ctx, in)
}

func (d *RemoteDomainClient) Ready(ctx context.Context, in *packet.ReadyRequest) (*packet.Sync, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.inner.Ready(ctx, in)
}

func (d *RemoteDomainClient) WaitForReplay(ctx context.Context, in *packet.WaitForReplayRequest) (*packet.Sync, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.inner.WaitForReplay(ctx, in)
}

func (d *RemoteDomainClient) Close() {
	d.inner.DRPCConn().Close()
}

func NewRemoteDomainClient(dom dataflow.DomainIdx, shard uint, addr string) (DomainClient, error) {
	// Domain processing is always sequential so this doesn't need to be a concurrent domain client
	// TODO: this does need to support resiliency for network hiccups, i.e. reconnections
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	inner := packet.NewDRPCDomainClient(drpcconn.New(conn))
	return &RemoteDomainClient{inner: inner, targetDomain: dom, targetShard: shard}, nil
}

type Sender func(ctx context.Context, d DomainClient) (any, error)

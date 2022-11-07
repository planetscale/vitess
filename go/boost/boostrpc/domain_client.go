package boostrpc

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"

	"storj.io/drpc/drpcconn"
	"storj.io/drpc/drpcmetadata"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/dataflow/trace"
)

type DomainClient interface {
	ProcessAsync(ctx context.Context, in *boostpb.Packet) error
	ProcessSync(ctx context.Context, in *boostpb.SyncPacket) error
	Close()
}

type RemoteDomainClient struct {
	mu           sync.Mutex
	inner        boostpb.DRPCInnerDomainClient
	targetDomain boostpb.DomainIndex
	targetShard  uint
}

func (d *RemoteDomainClient) ProcessAsync(ctx context.Context, in *boostpb.Packet) error {
	if !d.mu.TryLock() {
		panic("RemoteDomainClient should never race")
	}
	defer d.mu.Unlock()

	if trace.T && trace.HasSpan(ctx) {
		in.Id = rand.Int31()
		trace.GetSpan(ctx).StartFlow("Packet RPC", in.Id, map[string]any{
			"packet":        in,
			"target_domain": d.targetDomain,
			"target_shard":  d.targetShard,
		})
	}

	ctx = drpcmetadata.Add(ctx, "target_domain", fmt.Sprintf("%d.%d", d.targetDomain, d.targetShard))
	_, err := d.inner.ProcessAsync(ctx, in)
	return err
}

func (d *RemoteDomainClient) ProcessSync(ctx context.Context, in *boostpb.SyncPacket) error {
	if !d.mu.TryLock() {
		panic("RemoteDomainClient should never race")
	}
	defer d.mu.Unlock()

	_, err := d.inner.ProcessSync(ctx, in)
	return err
}

func (d *RemoteDomainClient) Close() {
	d.inner.DRPCConn().Close()
}

func NewRemoteDomainClient(dom boostpb.DomainIndex, shard uint, addr string) (DomainClient, error) {
	// Domain processing is always sequential so this doesn't need to be a concurrent domain client
	// TODO: this does need to support resiliency for network hiccups, i.e. reconnections
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	inner := boostpb.NewDRPCInnerDomainClient(drpcconn.New(conn))
	return &RemoteDomainClient{inner: inner, targetDomain: dom, targetShard: shard}, nil
}

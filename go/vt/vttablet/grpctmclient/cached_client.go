/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package grpctmclient

import (
	"context"
	"flag"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc"

	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	tabletmanagerservicepb "vitess.io/vitess/go/vt/proto/tabletmanagerservice"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	defaultPoolCapacity      = flag.Int("tablet_manager_grpc_connpool_size", 10, "number of tablets to keep tmclient connections open to")
	defaultPoolIdleTimeout   = flag.Duration("tablet_manager_grpc_connpool_idle_timeout", time.Second*30, "how long to leave a connection in the tmclient connpool. acquiring a connection resets this period for that connection")
	defaultPoolWaitTimeout   = flag.Duration("tablet_manager_grpc_connpool_wait_timeout", time.Millisecond*50, "how long to wait for a connection from the tmclient connpool")
	defaultPoolSweepInterval = flag.Duration("tablet_manager_grpc_connpool_sweep_interval", time.Second*30, "how often to clean up and close unused tmclient connections that exceed the idle timeout")
)

func init() {
	tmclient.RegisterTabletManagerClientFactory("grpc-cached", func() tmclient.TabletManagerClient {
		return NewCachedClient(*defaultPoolCapacity, *defaultPoolIdleTimeout, *defaultPoolWaitTimeout, *defaultPoolSweepInterval)
	})
}

type pooledTMC struct {
	tabletmanagerservicepb.TabletManagerClient
	cc *grpc.ClientConn

	m              sync.Mutex // protects lastAccessTime and refs
	lastAccessTime time.Time
	refs           int
}

// newPooledConn returns a pooledTMC dialed to the given address, and with the
// equivalent of having called aquire() on it exactly once.
func newPooledConn(addr string) (*pooledTMC, error) {
	opt, err := grpcclient.SecureDialOption(*cert, *key, *ca, *name)
	if err != nil {
		return nil, err
	}

	cc, err := grpcclient.Dial(addr, grpcclient.FailFast(false), opt)
	if err != nil {
		return nil, err
	}

	return &pooledTMC{
		TabletManagerClient: tabletmanagerservicepb.NewTabletManagerClient(cc),
		cc:                  cc,
		lastAccessTime:      time.Now(),
		refs:                1,
	}, nil
}

func (tmc *pooledTMC) acquire() {
	tmc.m.Lock()
	defer tmc.m.Unlock()

	tmc.refs++
	tmc.lastAccessTime = time.Now()
}

func (tmc *pooledTMC) release() {
	tmc.m.Lock()
	defer tmc.m.Unlock()

	tmc.refs--
	if tmc.refs < 0 {
		panic("release() called on unacquired pooled tabletmanager conn")
	}
}

// Close implements io.Closer for a pooledTMC. It is a wrapper around release()
// which never returns an error, but will panic if called on a pooledTMC that
// has not been acquire()'d.
func (tmc *pooledTMC) Close() error {
	tmc.release()
	return nil
}

type cachedClient struct {
	capacity    int
	idleTimeout time.Duration
	waitTimeout time.Duration

	// freeCh is used to signal that a slot in the conns map has been freed up
	freeCh chan *struct{}

	m     sync.Mutex // protects conns map
	conns map[string]*pooledTMC

	sweepTimer *timer.Timer
}

// NewCachedClient returns a Client using the cachedClient dialer implementation.
// Because all connections are cached/pooled, it does not implement poolDialer,
// and grpctmclient.Client will use pooled connections for all RPCs.
func NewCachedClient(capacity int, idleTimeout time.Duration, waitTimeout time.Duration, sweepInterval time.Duration) *Client {
	cc := &cachedClient{
		capacity:    capacity,
		idleTimeout: idleTimeout,
		waitTimeout: waitTimeout,
		freeCh:      make(chan *struct{}, capacity),
		conns:       make(map[string]*pooledTMC, capacity),
		sweepTimer:  timer.NewTimer(sweepInterval),
	}

	// mark all slots as open
	for i := 0; i < cc.capacity; i++ {
		cc.freeCh <- nil
	}

	cc.sweepTimer.Start(cc.sweep)
	return &Client{
		dialer: cc,
	}
}

func (client *cachedClient) dial(ctx context.Context, tablet *topodatapb.Tablet) (tabletmanagerservicepb.TabletManagerClient, io.Closer, error) {
	addr := getTabletAddr(tablet)

	client.m.Lock()
	if conn, ok := client.conns[addr]; ok {
		// Fast path, we have a conn for this addr in the cache. Mark it as
		// acquired and return it.
		defer client.m.Unlock()
		conn.acquire()

		return conn, conn, nil
	}
	client.m.Unlock()

	// Slow path, we're going to see if there's a free slot. If so, we'll claim
	// it and dial a new conn. If not, we're going to have to wait or timeout.
	// We don't hold the lock while we're polling.
	ctx, cancel := context.WithTimeout(ctx, client.waitTimeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err() // TODO: wrap
		case <-client.freeCh:
			// TODO: if something isn't immediately free, we should try to trigger the
			// sweeper, but we'll need another structure to make sure we aren't constantly
			// doing that; some sort of backoff/grace period.
			//
			// Also, if we're doing that, we probably want the sweeper to evict
			// the first conn with no refs, and ignore IdleTimeout. No one's
			// using that conn, and we want to use this one, so we shall.
			client.m.Lock()
			select {
			case <-ctx.Done():
				// context expired while we were waiting for the lock, relinquish
				// our spot in the freeCh
				client.m.Unlock()
				client.freeCh <- nil
				return nil, nil, ctx.Err() // TODO: wrap
			default:
			}
			// Time to get a new conn. We will return from the end of this
			// section no matter what.
			defer client.m.Unlock()
			conn, err := newPooledConn(addr)
			if err != nil {
				client.freeCh <- nil
				return nil, nil, err
			}

			client.conns[addr] = conn
			return conn, conn, nil
		default:
			client.m.Lock()
			client.sweepFnLocked(func(conn *pooledTMC) (bool, bool) {
				if conn.refs == 0 {
					// We only want to sweep far enough to free one conn, then
					// stop and let the rest of the sweeping happen in the
					// background timer.
					return true, true
				}

				return false, false
			})
			client.m.Unlock()
		}
	}
}

func (client *cachedClient) Close() {
	client.m.Lock()
	defer client.m.Unlock()

	client.sweepTimer.Stop()
	client.sweepFnLocked(func(conn *pooledTMC) (bool, bool) {
		return true, false
	})
}

func (client *cachedClient) sweep() {
	client.m.Lock()
	defer client.m.Unlock()

	now := time.Now()
	client.sweepFnLocked(func(conn *pooledTMC) (bool, bool) {
		return conn.refs == 0 && conn.lastAccessTime.Add(client.idleTimeout).Before(now), false
	})
}

func (client *cachedClient) sweepFnLocked(f func(conn *pooledTMC) (shouldFree bool, stopSweep bool)) {
	for key, conn := range client.conns {
		conn.m.Lock()
		shouldFree, stopSweep := f(conn)
		if !shouldFree {
			conn.m.Unlock()
			if stopSweep {
				return
			}

			continue
		}

		conn.cc.Close()
		client.freeCh <- nil
		delete(client.conns, key)
		conn.m.Unlock()

		if stopSweep {
			return
		}
	}
}

func getTabletAddr(tablet *topodatapb.Tablet) string {
	return netutil.JoinHostPort(tablet.Hostname, int32(tablet.PortMap["grpc"]))
}

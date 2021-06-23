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
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc"

	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	tabletmanagerservicepb "vitess.io/vitess/go/vt/proto/tabletmanagerservice"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	defaultPoolCapacity = flag.Int("tablet_manager_grpc_connpool_size", 100, "number of tablets to keep tmclient connections open to")
)

func init() {
	tmclient.RegisterTabletManagerClientFactory("grpc-cached", func() tmclient.TabletManagerClient {
		return NewCachedConnClient(*defaultPoolCapacity)
	})
}

// closeFunc allows a standalone function to implement io.Closer, similar to
// how http.HandlerFunc allows standalone functions to implement http.Handler.
type closeFunc func() error

func (fn closeFunc) Close() error {
	return fn()
}

var _ io.Closer = (*closeFunc)(nil)

type cachedConn struct {
	tabletmanagerservicepb.TabletManagerClient
	cc *grpc.ClientConn

	addr           string
	lastAccessTime time.Time
	refs           int
}

type cachedConnDialer struct {
	m            sync.Mutex
	conns        map[string]*cachedConn
	evict        []*cachedConn
	evictSorted  bool
	connWaitSema *sync2.Semaphore
}

var dialerStats = struct {
	ConnReuse    *stats.Gauge
	ConnNew      *stats.Gauge
	DialTimeouts *stats.Gauge
	DialTimings  *stats.Timings
}{
	ConnReuse:    stats.NewGauge("tabletmanagerclient_cachedconn_reuse", "number of times a call to dial() was able to reuse an existing connection"),
	ConnNew:      stats.NewGauge("tabletmanagerclient_cachedconn_new", "number of times a call to dial() resulted in a dialing a new grpc clientconn"),
	DialTimeouts: stats.NewGauge("tabletmanagerclient_cachedconn_dial_timeouts", "number of context timeouts during dial()"),
	DialTimings:  stats.NewTimings("tabletmanagerclient_cachedconn_dialtimings", "timings for various dial paths", "path", "rlock_fast", "sema_fast", "sema_poll"),
}

// NewCachedConnClient returns a grpc Client that caches connections to the different tablets
func NewCachedConnClient(capacity int) *Client {
	dialer := &cachedConnDialer{
		conns:        make(map[string]*cachedConn, capacity),
		evict:        make([]*cachedConn, 0, capacity),
		connWaitSema: sync2.NewSemaphore(capacity, 0),
	}
	return &Client{dialer}
}

var _ dialer = (*cachedConnDialer)(nil)

func (dialer *cachedConnDialer) sortEvictionsLocked() {
	if !dialer.evictSorted {
		sort.Slice(dialer.evict, func(i, j int) bool {
			left, right := dialer.evict[i], dialer.evict[j]
			if left.refs == right.refs {
				return left.lastAccessTime.After(right.lastAccessTime)
			}
			return left.refs > right.refs
		})
		dialer.evictSorted = true
	}
}

func (dialer *cachedConnDialer) dial(ctx context.Context, tablet *topodatapb.Tablet) (tabletmanagerservicepb.TabletManagerClient, io.Closer, error) {
	start := time.Now()

	addr := getTabletAddr(tablet)
	dialer.m.Lock()
	if conn, ok := dialer.conns[addr]; ok {
		defer func() {
			dialerStats.DialTimings.Add("lock_fast", time.Since(start))
		}()
		defer dialer.m.Unlock()
		return dialer.redialLocked(conn)
	}
	dialer.m.Unlock()

	if dialer.connWaitSema.TryAcquire() {
		defer func() {
			dialerStats.DialTimings.Add("sema_fast", time.Since(start))
		}()

		dialer.m.Lock()
		// Check if another goroutine managed to dial a conn for the same addr
		// while we were waiting for the write lock. This is identical to the
		// read-lock section above.
		if conn, ok := dialer.conns[addr]; ok {
			return dialer.redialLocked(conn)
		}
		dialer.m.Unlock()

		return dialer.newdial(addr)
	}

	defer func() {
		dialerStats.DialTimings.Add("sema_poll", time.Since(start))
	}()

	for {
		select {
		case <-ctx.Done():
			dialerStats.DialTimeouts.Add(1)
			return nil, nil, ctx.Err()
		default:
			dialer.m.Lock()
			if conn, ok := dialer.conns[addr]; ok {
				// Someone else dialed this addr while we were polling. No need
				// to evict anyone else, just reuse the existing conn.
				defer dialer.m.Unlock()
				return dialer.redialLocked(conn)
			}

			dialer.sortEvictionsLocked()

			conn := dialer.evict[len(dialer.evict)-1]
			if conn.refs != 0 {
				dialer.m.Unlock()
				continue
			}

			// We're going to return from this point
			dialer.evict = dialer.evict[:len(dialer.evict)-1]
			delete(dialer.conns, conn.addr)
			conn.cc.Close()
			dialer.m.Unlock()

			return dialer.newdial(addr)
		}
	}
}

// newdial creates a new cached connection, and updates the cache and eviction
// queue accordingly. If newdial fails to create the underlying
// gRPC connection, it will make a call to Release the connWaitSema for other
// newdial calls.
//
// It returns the three-tuple of client-interface, closer, and error that the
// main dial func returns.
func (dialer *cachedConnDialer) newdial(addr string) (tabletmanagerservicepb.TabletManagerClient, io.Closer, error) {
	dialerStats.ConnNew.Add(1)

	opt, err := grpcclient.SecureDialOption(*cert, *key, *ca, *name)
	if err != nil {
		dialer.connWaitSema.Release()
		return nil, nil, err
	}

	cc, err := grpcclient.Dial(addr, grpcclient.FailFast(false), opt)
	if err != nil {
		dialer.connWaitSema.Release()
		return nil, nil, err
	}

	dialer.m.Lock()
	defer dialer.m.Unlock()

	if conn, existing := dialer.conns[addr]; existing {
		// race condition: some other goroutine has dialed our tablet before we have;
		// this is not great, but shouldn't happen often (if at all), so we're going to
		// close this connection and reuse the existing one. by doing this, we can keep
		// the actual Dial out of the global lock and significantly increase throughput
		cc.Close()
		return dialer.redialLocked(conn)
	}

	conn := &cachedConn{
		TabletManagerClient: tabletmanagerservicepb.NewTabletManagerClient(cc),
		cc:                  cc,
		lastAccessTime:      time.Now(),
		refs:                1,
		addr:                addr,
	}
	dialer.evict = append(dialer.evict, conn)
	dialer.evictSorted = false
	dialer.conns[addr] = conn

	return dialer.connWithCloser(conn)
}

// redialLocked takes an already-dialed connection in the cache does all the work of
// lending that connection out to one more caller. this should only ever be
// called while holding at least the RLock on dialer.m (but the write lock is
// fine too), to prevent the connection from getting evicted out from under us.
//
// It returns the three-tuple of client-interface, closer, and error that the
// main dial func returns.
func (dialer *cachedConnDialer) redialLocked(conn *cachedConn) (tabletmanagerservicepb.TabletManagerClient, io.Closer, error) {
	dialerStats.ConnReuse.Add(1)
	conn.lastAccessTime = time.Now()
	conn.refs++
	return dialer.connWithCloser(conn)
}

// connWithCloser returns the three-tuple expected by the main dial func, where
// the closer handles the correct state management for updating the conns place
// in the eviction queue.
func (dialer *cachedConnDialer) connWithCloser(conn *cachedConn) (tabletmanagerservicepb.TabletManagerClient, io.Closer, error) {
	return conn, closeFunc(func() error {
		dialer.m.Lock()
		defer dialer.m.Unlock()
		conn.refs--
		return nil
	}), nil
}

// Close closes all currently cached connections, ***regardless of whether
// those connections are in use***. Calling Close therefore will fail any RPCs
// using currently lent-out connections, and, furthermore, will invalidate the
// io.Closer that was returned for that connection from dialer.dial().
//
// As a result, it is not safe to reuse a cachedConnDialer after calling Close,
// and you should instead obtain a new one by calling either
// tmclient.TabletManagerClient() with
// TabletManagerProtocol set to "grpc-cached", or by calling
// grpctmclient.NewCachedConnClient directly.
func (dialer *cachedConnDialer) Close() {
	dialer.m.Lock()
	defer dialer.m.Unlock()

	for _, conn := range dialer.evict {
		conn.cc.Close()
		delete(dialer.conns, conn.addr)
		dialer.connWaitSema.Release()
	}
	dialer.evict = nil
}

func getTabletAddr(tablet *topodatapb.Tablet) string {
	return netutil.JoinHostPort(tablet.Hostname, int32(tablet.PortMap["grpc"]))
}

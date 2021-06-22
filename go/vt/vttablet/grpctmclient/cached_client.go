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
	"container/heap"
	"context"
	"flag"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc"

	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/sync2"
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
	tmclient.RegisterTabletManagerClientFactory("grpc-cached-pqueue", func() tmclient.TabletManagerClient {
		return NewCachedConnClient(*defaultPoolCapacity)
	})
}

type pooledTMC struct {
	tabletmanagerservicepb.TabletManagerClient
	cc *grpc.ClientConn

	m              sync.RWMutex // protects lastAccessTime and refs
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

func dialCommon(addr string) (*grpc.ClientConn, error) {
	// sharing code between my two implementations because i'm lazy
	opt, err := grpcclient.SecureDialOption(*cert, *key, *ca, *name)
	if err != nil {
		return nil, err
	}

	cc, err := grpcclient.Dial(addr, grpcclient.FailFast(false), opt)
	if err != nil {
		return nil, err
	}

	return cc, nil
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

type closeFunc func() error

func (fn closeFunc) Close() error {
	return fn()
}

var _ io.Closer = (*closeFunc)(nil)

type cachedClient struct {
	capacity    int
	idleTimeout time.Duration
	waitTimeout time.Duration

	// sema gates the addition of new connections to the cache
	sema *sync2.Semaphore

	m     sync.RWMutex // protects conns map
	conns map[string]*pooledTMC

	janitor *janitor
}

// NewCachedClient returns a Client using the cachedClient dialer implementation.
// Because all connections are cached/pooled, it does not implement poolDialer,
// and grpctmclient.Client will use pooled connections for all RPCs.
func NewCachedClient(capacity int, idleTimeout time.Duration, waitTimeout time.Duration, sweepInterval time.Duration) *Client {
	cc := &cachedClient{
		capacity:    capacity,
		idleTimeout: idleTimeout,
		waitTimeout: waitTimeout,
		sema:        sync2.NewSemaphore(capacity, waitTimeout),
		conns:       make(map[string]*pooledTMC, capacity),
		janitor: &janitor{
			ch:    make(chan *struct{}, 10), // TODO: flag
			timer: timer.NewTimer(sweepInterval),
		},
	}

	cc.janitor.client = cc
	go cc.janitor.run()

	return &Client{
		dialer: cc,
	}
}

func (client *cachedClient) dial(ctx context.Context, tablet *topodatapb.Tablet) (tabletmanagerservicepb.TabletManagerClient, io.Closer, error) {
	addr := getTabletAddr(tablet)

	client.m.RLock()
	if conn, ok := client.conns[addr]; ok {
		// Fast path, we have a conn for this addr in the cache. Mark it as
		// acquired and return it.
		defer client.m.RUnlock()
		conn.acquire()

		return conn, conn, nil
	}
	client.m.RUnlock()

	// Slow path, we're going to see if there's a free slot. If so, we'll claim
	// it and dial a new conn. If not, we're going to have to wait or timeout.
	// We don't hold the lock while we're polling.
	ctx, cancel := context.WithTimeout(ctx, client.waitTimeout)
	defer cancel()

	dial := func(addr string) (conn *pooledTMC, closer io.Closer, err error) {
		client.m.Lock()
		defer client.m.Unlock()

		defer func() {
			// If we failed to dial a new conn for any reason, release our spot
			// in the sema so another dial can take its place.
			if err != nil {
				client.sema.Release()
			}
		}()

		select {
		case <-ctx.Done(): // We timed out waiting for the write lock, bail.
			return nil, nil, ctx.Err() // TODO: wrap
		default:
		}

		conn, err = newPooledConn(addr)
		if err != nil {
			return nil, nil, err
		}

		client.conns[addr] = conn
		return conn, conn, nil
	}

	if client.sema.TryAcquire() {
		return dial(addr)
	}

	select {
	// Non-blocking signal to the janitor that it should consider sweeping soon.
	case client.janitor.ch <- sentinel:
	default:
	}

	if !client.sema.AcquireContext(ctx) {
		return nil, nil, ctx.Err()
	}

	return dial(addr)
}

func (client *cachedClient) Close() {
	client.m.Lock()
	defer client.m.Unlock()

	close(client.janitor.ch)
	client.sweepFnLocked(func(conn *pooledTMC) (bool, bool) {
		return true, false
	})
}

func (client *cachedClient) sweep2(f func(conn *pooledTMC) (bool, bool)) {
	client.m.RLock()

	var toFree []string
	for key, conn := range client.conns {
		conn.m.RLock()
		shouldFree, stopSweep := f(conn)
		conn.m.RUnlock()

		if shouldFree {
			toFree = append(toFree, key)
		}

		if stopSweep {
			break
		}
	}

	client.m.RUnlock()

	if len(toFree) > 0 {
		client.m.Lock()
		defer client.m.Unlock()

		for _, key := range toFree {
			conn, ok := client.conns[key]
			if !ok {
				continue
			}

			conn.m.Lock()
			// check the condition again, things may have changed since we
			// transitioned from the read lock to the write lock
			shouldFree, _ := f(conn)
			if !shouldFree {
				conn.m.Unlock()
				continue
			}

			conn.cc.Close()
			conn.m.Unlock()
			delete(client.conns, key)
			client.sema.Release()
		}
	}
}

func (client *cachedClient) sweep() {
	now := time.Now()
	client.sweep2(func(conn *pooledTMC) (bool, bool) {
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
		delete(client.conns, key)
		client.sema.Release()
		conn.m.Unlock()

		if stopSweep {
			return
		}
	}
}

var sentinel = &struct{}{}

type janitor struct {
	ch     chan *struct{}
	client *cachedClient
	timer  *timer.Timer

	m         sync.Mutex
	sweeping  bool
	lastSweep time.Time
}

func (j *janitor) run() {
	j.timer.Start(j.sweep)
	defer j.timer.Stop()

	for s := range j.ch {
		if s == nil {
			break
		}

		scan := true
		t := time.NewTimer(time.Millisecond * 50) // TODO: flag
		for scan {
			select {
			case <-t.C:
				scan = false
			case s := <-j.ch:
				if s == nil {
					scan = false
				}
			default:
				scan = false
			}
		}

		t.Stop()
		j.sweep()
	}
}

func (j *janitor) sweep() {
	j.m.Lock()
	if j.sweeping {
		j.m.Unlock()
		return
	}

	if j.lastSweep.Add(time.Millisecond * 10 /* TODO: flag */).After(time.Now()) {
		j.m.Unlock()
		return
	}

	j.sweeping = true
	j.m.Unlock()

	j.client.sweep()
	j.m.Lock()
	j.sweeping = false
	j.lastSweep = time.Now()
	j.m.Unlock()
}

type cachedConn struct {
	tabletmanagerservicepb.TabletManagerClient
	cc *grpc.ClientConn

	lastAccessTime time.Time
	refs           int

	index int
	key   string
}

type cachedConns []*cachedConn

var _ heap.Interface = (*cachedConns)(nil)

func (queue cachedConns) Len() int { return len(queue) }

func (queue cachedConns) Less(i, j int) bool {
	left, right := queue[i], queue[j]
	if left.refs == right.refs {
		// break ties by access time.
		// more stale connections have higher priority for removal
		// this condition is equvalent to:
		//		left.lastAccessTime <= right.lastAccessTime
		return !left.lastAccessTime.After(right.lastAccessTime)
	}

	// connections with fewer refs have higher priority for removal
	return left.refs < right.refs
}

func (queue cachedConns) Swap(i, j int) {
	queue[i], queue[j] = queue[j], queue[i]
	queue[i].index = i
	queue[j].index = j
}

func (queue *cachedConns) Push(x interface{}) {
	n := len(*queue)
	conn := x.(*cachedConn)
	conn.index = n
	*queue = append(*queue, conn)
}

func (queue *cachedConns) Pop() interface{} {
	old := *queue
	n := len(old)
	conn := old[n-1]
	old[n-1] = nil  // avoid memory leak
	conn.index = -1 // for safety
	*queue = old[0 : n-1]

	return conn
}

type cachedConnDialer struct {
	m            sync.RWMutex
	conns        map[string]*cachedConn
	qMu          sync.Mutex
	queue        cachedConns
	connWaitSema *sync2.Semaphore
}

// NewCachedConnClient returns a grpc Client using the priority queue cache
// dialer implementation.
func NewCachedConnClient(capacity int) *Client {
	dialer := &cachedConnDialer{
		conns:        make(map[string]*cachedConn, capacity),
		queue:        make(cachedConns, 0, capacity),
		connWaitSema: sync2.NewSemaphore(capacity, 0),
	}

	heap.Init(&dialer.queue)
	return &Client{dialer}
}

var _ dialer = (*cachedConnDialer)(nil)

func (dialer *cachedConnDialer) dial(ctx context.Context, tablet *topodatapb.Tablet) (tabletmanagerservicepb.TabletManagerClient, io.Closer, error) {
	addr := getTabletAddr(tablet)
	dialer.m.RLock()
	if conn, ok := dialer.conns[addr]; ok {
		defer dialer.m.RUnlock()
		return dialer.redial(conn)
	}
	dialer.m.RUnlock()

	if dialer.connWaitSema.TryAcquire() {
		dialer.m.Lock()
		defer dialer.m.Unlock()

		// Check if another goroutine managed to dial a conn for the same addr
		// while we were waiting for the write lock. This is identical to the
		// read-lock section above.
		if conn, ok := dialer.conns[addr]; ok {
			return dialer.redial(conn)
		}

		return dialer.newdial(addr, true /* manage queue lock */)
	}

	for {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		default:
			dialer.m.Lock()
			dialer.qMu.Lock()
			conn := dialer.queue[0]
			if conn.refs != 0 {
				dialer.qMu.Unlock()
				dialer.m.Unlock()
				continue
			}

			// We're going to return from this point
			defer dialer.m.Unlock()
			defer dialer.qMu.Unlock()
			heap.Pop(&dialer.queue)
			delete(dialer.conns, conn.key)
			conn.cc.Close()

			return dialer.newdial(addr, false /* manage queue lock */)
		}
	}
}

// newdial creates a new cached connection, and updates the cache and eviction
// queue accordingly. This must be called only while holding the write lock on
// dialer.m as well as after having successfully acquired the dialer.connWaitSema. If newdial fails to create the underlying
// gRPC connection, it will make a call to Release the connWaitSema for other
// newdial calls.
//
// It returns the three-tuple of client-interface, closer, and error that the
// main dial func returns.
func (dialer *cachedConnDialer) newdial(addr string, manageQueueLock bool) (tabletmanagerservicepb.TabletManagerClient, io.Closer, error) {
	cc, err := dialCommon(addr)
	if err != nil {
		dialer.connWaitSema.Release()
		return nil, nil, err
	}

	// In the case where dial is evicting a connection from the cache, we
	// already have a lock on the eviction queue. Conversely, in the case where
	// we are able to create a new connection without evicting (because the
	// cache is not yet full), we don't have the queue lock yet.
	if manageQueueLock {
		dialer.qMu.Lock()
		defer dialer.qMu.Unlock()
	}

	conn := &cachedConn{
		TabletManagerClient: tabletmanagerservicepb.NewTabletManagerClient(cc),
		cc:                  cc,
		lastAccessTime:      time.Now(),
		refs:                1,
		index:               -1, // gets set by call to Push
		key:                 addr,
	}
	heap.Push(&dialer.queue, conn)
	dialer.conns[addr] = conn

	return dialer.connWithCloser(conn)
}

// redial takes an already-dialed connection in the cache does all the work of
// lending that connection out to one more caller. this should only ever be
// called while holding at least the RLock on dialer.m (but the write lock is
// fine too), to prevent the connection from getting evicted out from under us.
//
// It returns the three-tuple of client-interface, closer, and error that the
// main dial func returns.
func (dialer *cachedConnDialer) redial(conn *cachedConn) (tabletmanagerservicepb.TabletManagerClient, io.Closer, error) {
	dialer.qMu.Lock()
	defer dialer.qMu.Unlock()

	conn.lastAccessTime = time.Now()
	conn.refs++
	heap.Fix(&dialer.queue, conn.index)

	return dialer.connWithCloser(conn)
}

func (dialer *cachedConnDialer) connWithCloser(conn *cachedConn) (tabletmanagerservicepb.TabletManagerClient, io.Closer, error) {
	return conn, closeFunc(func() error {
		dialer.qMu.Lock()
		defer dialer.qMu.Unlock()

		conn.refs--
		heap.Fix(&dialer.queue, conn.index)
		return nil
	}), nil
}

func (dialer *cachedConnDialer) Close() {
	dialer.m.Lock()
	defer dialer.m.Unlock()
	dialer.qMu.Lock()
	defer dialer.qMu.Unlock()

	for dialer.queue.Len() > 0 {
		conn := dialer.queue.Pop().(*cachedConn)
		conn.cc.Close()
		delete(dialer.conns, conn.key)
	}
}

func getTabletAddr(tablet *topodatapb.Tablet) string {
	return netutil.JoinHostPort(tablet.Hostname, int32(tablet.PortMap["grpc"]))
}

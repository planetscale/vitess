package drpc

import (
	"context"
	"crypto/tls"
	"net"
	"sync"

	"go.uber.org/zap"
	"storj.io/drpc"
	"storj.io/drpc/drpcconn"
	"storj.io/drpc/drpcmigrate"

	clmw "vitess.io/vitess/go/boost/boostrpc/drpc/middleware/client"
)

// Client represents an instance used to consume DRPC services offered
// by other entities in the network.
type Client struct {
	ntp      string                // network protocol used
	tls      *tls.Config           // TLS settings
	mtx      sync.Mutex            // concurrent access lock
	ctx      context.Context       // main context
	log      *zap.Logger           // client logger
	mdw      []clmw.Middleware     // middleware set
	halt     context.CancelFunc    // halt notification trigger
	capacity int                   // pool connection capacity
	cache    *pool[*drpcconn.Conn] // DRPC connection pool
	closed   chan struct{}         // already closed flag
	addr     string                // user-provided network endpoint
	http     bool                  // HTTP support-enabled flag
}

// NewClient returns a ready-to-use DRPC client instance.
func NewClient(network, address string, options ...ClientOption) (*Client, error) {
	ctx, halt := context.WithCancel(context.Background())
	cl := &Client{
		ntp:      network,
		ctx:      ctx,
		log:      zap.NewNop(),
		mdw:      []clmw.Middleware{},
		halt:     halt,
		addr:     address,
		closed:   make(chan struct{}),
		capacity: 1,
	}
	if err := cl.setup(options...); err != nil {
		return nil, err
	}
	cl.cache = &pool[*drpcconn.Conn]{
		limit: cl.capacity,
		free: func(conn *drpcconn.Conn) error {
			return conn.Close()
		},
		new: func() (*drpcconn.Conn, error) {
			nc, err := cl.dial()
			if err != nil {
				return nil, err
			}
			return drpcconn.New(nc), nil
		},
	}
	return cl, nil
}

// Close the client and free related resources.
func (cl *Client) Close() error {
	cl.mtx.Lock()
	defer cl.mtx.Unlock()
	for err := range cl.cache.Drain() {
		cl.log.Warn("failed to drain connection", zap.Error(err))
	}
	close(cl.closed)
	return nil
}

// Closed returns a channel that is closed when the client is definitely closed.
func (cl *Client) Closed() <-chan struct{} {
	cl.mtx.Lock()
	defer cl.mtx.Unlock()
	return cl.closed
}

// IsActive returns true if the client has any active DRPC connection.
func (cl *Client) IsActive() bool {
	cl.mtx.Lock()
	defer cl.mtx.Unlock()
	_, act := cl.cache.Stats()
	return act > 0
}

// Transport returns nil because it does not have a fixed transport.
func (cl *Client) Transport() drpc.Transport {
	return nil
}

// Use will register middleware elements to be applied to the client instance.
// Middleware is executed before the processing of RPC requests is started.
// When providing middleware the ordering is very important; middleware will be
// applied in the same order provided.
//
//	For example:
//	  Use(foo bar baz)
//	Will be applied as:
//	  baz( bar( foo(handler) ) )
func (cl *Client) Use(mw ...clmw.Middleware) {
	cl.mtx.Lock()
	for _, m := range mw {
		cl.mdw = append([]clmw.Middleware{m}, cl.mdw...)
	}
	cl.mtx.Unlock()
}

// Invoke acquires a connection from the pool, dialing if necessary, and
// issues a unary RPC to the remote on that connection. The connection is
// put back into the pool after the operation finishes.
func (cl *Client) Invoke(ctx context.Context, rpc string, enc drpc.Encoding, in, out drpc.Message) error {
	// Get connection from the pool
	conn, err := cl.cache.Get()
	if err != nil {
		return err
	}
	defer cl.cache.Put(conn)

	var handler clmw.Interceptor = conn
	for _, mw := range cl.mdw {
		handler = mw(handler)
	}
	return handler.Invoke(ctx, rpc, enc, in, out)
}

// NewStream acquires a connection from the pool, dialing if necessary, and
// starts a stream with the remote on that connection. The connection is put
// back into the pool after the stream is finished.
func (cl *Client) NewStream(ctx context.Context, rpc string, enc drpc.Encoding) (drpc.Stream, error) {
	// Get connection from the pool
	conn, err := cl.cache.Get()
	if err != nil {
		return nil, err
	}
	defer cl.cache.Put(conn)

	var handler clmw.Interceptor = conn
	for _, mw := range cl.mdw {
		handler = mw(handler)
	}
	return handler.NewStream(ctx, rpc, enc)
}

// Apply user provided configuration options.
func (cl *Client) setup(options ...ClientOption) (err error) {
	for _, opt := range options {
		if err = opt(cl); err != nil {
			return
		}
	}
	return
}

// Setup client's main network connection.
func (cl *Client) dial() (nc net.Conn, err error) {
	nc, err = net.Dial(cl.ntp, cl.addr)
	if err != nil {
		return
	}

	if cl.tls != nil {
		nc = tls.Client(nc, cl.tls)
	}

	if cl.http {
		nc = drpcmigrate.NewHeaderConn(nc, drpcmigrate.DRPCHeader)
	}

	return
}

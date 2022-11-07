package client

import (
	"context"

	"storj.io/drpc"
)

// Interceptor defines a simplified version of the `drpcconn.Conn`
// interface for elements wishing to extend the client's RPC processing
// functionality using middleware pattern.
type Interceptor interface {
	// Invoke issues a unary RPC to the remote.
	Invoke(ctx context.Context, rpc string, enc drpc.Encoding, in, out drpc.Message) error

	// NewStream starts a stream with the remote.
	NewStream(ctx context.Context, rpc string, enc drpc.Encoding) (drpc.Stream, error)
}

// Middleware elements allow to customize and extend the RPC requests
// processing by the client.
type Middleware func(Interceptor) Interceptor

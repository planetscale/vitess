package client

import (
	"context"
	"fmt"

	"storj.io/drpc"
)

// PanicRecovery allows the client to convert unhandled panic events into an
// "internal" RPC error. This will prevent the client from crashing if a handler
// produces a `panic` operation.
func PanicRecovery() Middleware {
	return func(next Interceptor) Interceptor {
		return panicRecovery{
			tag:  "internal error",
			next: next,
		}
	}
}

type panicRecovery struct {
	tag  string
	next Interceptor
}

func (md panicRecovery) Invoke(ctx context.Context, rpc string, enc drpc.Encoding, in, out drpc.Message) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = fmt.Errorf("%s: %s", md.tag, v)
		}
	}()
	err = md.next.Invoke(ctx, rpc, enc, in, out)
	return
}

func (md panicRecovery) NewStream(ctx context.Context, rpc string, enc drpc.Encoding) (st drpc.Stream, err error) {
	defer func() {
		if v := recover(); v != nil {
			err = fmt.Errorf("%s: %s", md.tag, v)
		}
	}()
	st, err = md.next.NewStream(ctx, rpc, enc)
	return
}

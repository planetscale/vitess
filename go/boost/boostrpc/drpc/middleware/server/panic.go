package server

import (
	"fmt"

	"storj.io/drpc"
)

// PanicRecovery allows the server to convert unhandled panic events into an
// "internal" RPC error. This will prevent the server from crashing if a handler
// produces a `panic` operation.
func PanicRecovery() Middleware {
	return func(next drpc.Handler) drpc.Handler {
		return panicRecovery{
			tag:  "internal error",
			next: next,
		}
	}
}

type panicRecovery struct {
	next drpc.Handler
	tag  string
}

func (md panicRecovery) HandleRPC(stream drpc.Stream, rpc string) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = fmt.Errorf("%s: %s", md.tag, v)
		}
	}()
	err = md.next.HandleRPC(stream, rpc)
	return
}

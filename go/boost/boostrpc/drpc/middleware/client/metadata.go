package client

import (
	"context"

	"storj.io/drpc"
	"storj.io/drpc/drpcmetadata"
)

// Metadata middleware adds the provided payload data to the context
// of every request (unary and stream) before sending it to the server.
func Metadata(payload map[string]string) Middleware {
	return func(next Interceptor) Interceptor {
		return metadata{
			payload: payload,
			next:    next,
		}
	}
}

type metadata struct {
	payload map[string]string
	next    Interceptor
}

func (md metadata) Invoke(ctx context.Context, rpc string, enc drpc.Encoding, in, out drpc.Message) error {
	ctx = drpcmetadata.AddPairs(ctx, md.payload)
	return md.next.Invoke(ctx, rpc, enc, in, out)
}

func (md metadata) NewStream(ctx context.Context, rpc string, enc drpc.Encoding) (drpc.Stream, error) {
	ctx = drpcmetadata.AddPairs(ctx, md.payload)
	return md.next.NewStream(ctx, rpc, enc)
}

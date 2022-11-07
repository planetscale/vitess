package client

import (
	"context"
	"errors"

	"golang.org/x/time/rate"
	"storj.io/drpc"
)

// RateLimit enforce a maximum limit of RPC requests per-second on the
// client. It is implemented as a `token bucket` instance.
// More information: https://en.wikipedia.org/wiki/Token_bucket
func RateLimit(limit int) Middleware {
	return func(next Interceptor) Interceptor {
		return rateLimit{
			check: rate.NewLimiter(rate.Limit(limit), limit),
			next:  next,
		}
	}
}

type rateLimit struct {
	check *rate.Limiter
	next  Interceptor
}

func (md rateLimit) Invoke(ctx context.Context, rpc string, enc drpc.Encoding, in, out drpc.Message) error {
	if !md.check.Allow() {
		return errors.New("rate: limit exceeded")
	}
	return md.next.Invoke(ctx, rpc, enc, in, out)
}

func (md rateLimit) NewStream(ctx context.Context, rpc string, enc drpc.Encoding) (drpc.Stream, error) {
	if !md.check.Allow() {
		return nil, errors.New("rate: limit exceeded")
	}
	return md.next.NewStream(ctx, rpc, enc)
}

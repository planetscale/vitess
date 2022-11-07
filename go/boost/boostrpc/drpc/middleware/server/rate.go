package server

import (
	"errors"

	"golang.org/x/time/rate"
	"storj.io/drpc"
)

// RateLimit enforce a maximum limit of RPC requests per-second on the
// server. It is implemented as a `token bucket` instance.
// More information: https://en.wikipedia.org/wiki/Token_bucket
func RateLimit(limit int) Middleware {
	return func(next drpc.Handler) drpc.Handler {
		return rateLimit{
			check: rate.NewLimiter(rate.Limit(limit), limit),
			next:  next,
		}
	}
}

type rateLimit struct {
	check *rate.Limiter
	next  drpc.Handler
}

func (md rateLimit) HandleRPC(stream drpc.Stream, rpc string) error {
	if !md.check.Allow() {
		return errors.New("rate: limit exceeded")
	}
	return md.next.HandleRPC(stream, rpc)
}

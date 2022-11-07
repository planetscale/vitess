package client

import (
	"context"
	"time"

	"go.uber.org/zap"
	"storj.io/drpc"
)

// Retry failed requests up to the `max` number of attempts specified. Retried
// errors will be logged as warnings along with details about the specific
// attempt. Multiple tries introduce an increasingly longer backoff delay to
// account for transient failures on the remote. The specific delay for each
// attempt is calculated (in ms) as: `delay * (factor * attempt_number)`.
func Retry(max uint, ll *zap.Logger) Middleware {
	return func(next Interceptor) Interceptor {
		return retry{
			tries:  0,
			limit:  max,
			delay:  300,
			factor: 0.85,
			log:    ll,
			next:   next,
		}
	}
}

type retry struct {
	tries  uint        // number of attempts per-request
	limit  uint        // max number of tries
	factor float32     // backoff factor
	delay  uint        // initial delay value
	log    *zap.Logger // logger
	next   Interceptor // chained interceptor
}

func (md retry) Invoke(ctx context.Context, rpc string, enc drpc.Encoding, in, out drpc.Message) error {
	defer func() {
		// reset attempts counter
		md.tries = 0
	}()
	for {
		md.tries++
		err := md.next.Invoke(ctx, rpc, enc, in, out)
		if err == nil {
			return nil
		}

		// Operation fields
		var fields = []zap.Field{
			zap.Error(err),
			zap.Uint("retry.attempt", md.tries),
		}

		// Verify limit
		if md.tries == md.limit {
			md.log.Warn("retry: max attempts exceeded", fields...)
			return err
		}

		// Automatic retry with backoff factor
		// pause = delay * (factor * tries)
		pause := time.Duration(float32(md.delay)*(md.factor*float32(md.tries))) * time.Millisecond
		fields = append(fields, zap.Duration("retry.pause", pause))
		md.log.Debug("retry: delaying new attempt", fields...)
		time.Sleep(pause)
		md.log.Warn("retry: re-submitting request", fields...)
		continue
	}
}

func (md retry) NewStream(ctx context.Context, rpc string, enc drpc.Encoding) (drpc.Stream, error) {
	defer func() {
		// reset attempts counter
		md.tries = 0
	}()
	for {
		md.tries++
		st, err := md.next.NewStream(ctx, rpc, enc)
		if err == nil {
			return st, nil
		}

		// Operation fields
		var fields = []zap.Field{
			zap.Error(err),
			zap.Uint("retry.attempt", md.tries),
		}

		// Verify limit
		if md.tries == md.limit {
			md.log.Warn("retry: max attempts exceeded", fields...)
			return nil, err
		}

		// Automatic retry with backoff factor
		// pause = delay * (factor * tries)
		pause := time.Duration(float32(md.delay)*(md.factor*float32(md.tries))) * time.Millisecond
		fields = append(fields, zap.Duration("retry.pause", pause))
		md.log.Debug("retry: delaying new attempt", fields...)
		time.Sleep(pause)
		md.log.Warn("retry: re-submitting request", fields...)
		continue
	}
}

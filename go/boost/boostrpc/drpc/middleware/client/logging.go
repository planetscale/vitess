package client

import (
	"context"
	"strings"
	"time"

	"go.uber.org/zap"
	"storj.io/drpc"
	"storj.io/drpc/drpcmetadata"
)

// Logging produce output for the processed RPC requests tagged with
// standard ECS details by default. Fields can be extended by providing
// a hook function.
func Logging(logger *zap.Logger, hook LoggingHook) Middleware {
	return func(next Interceptor) Interceptor {
		return logging{
			ll:   logger,
			hook: hook,
			next: next,
		}
	}
}

// LoggingHook provides a mechanism to extend a message fields just
// before is submitted.
type LoggingHook func(ctx context.Context, rpc string, fields []zap.Field) []zap.Field

type logging struct {
	ll   *zap.Logger
	hook LoggingHook
	next Interceptor
}

func (md logging) Invoke(ctx context.Context, rpc string, enc drpc.Encoding, in, out drpc.Message) error {
	// Get basic request details
	fields := getFields(ctx, rpc)
	fields = append(fields, zap.String("rpc.kind", "unary"))
	if md.hook != nil {
		fields = md.hook(ctx, rpc, fields)
	}

	// Start message
	md.ll.Info(rpc, fields...)

	// Process request
	start := time.Now().UTC()
	err := md.next.Invoke(ctx, rpc, enc, in, out)
	end := time.Now().UTC()
	lapse := end.Sub(start)

	// Additional details
	fields = append(fields,
		zap.Duration("event.duration", lapse),
		zap.Time("event.start", start),
		zap.Time("event.end", end),
	)

	// End message
	if err != nil {
		fields = append(fields, zap.Error(err))
		md.ll.Error("Invoke failed", fields...)
	} else {
		md.ll.Info("Invoke completed", fields...)
	}
	return err
}

func (md logging) NewStream(ctx context.Context, rpc string, enc drpc.Encoding) (drpc.Stream, error) {
	// Get basic request details
	fields := getFields(ctx, rpc)
	fields = append(fields, zap.String("rpc.kind", "stream"))
	if md.hook != nil {
		fields = md.hook(ctx, rpc, fields)
	}

	// Start message
	md.ll.Info(rpc, fields...)

	// Process request
	start := time.Now().UTC()
	fields = append(fields, zap.Time("event.start", start))
	st, err := md.next.NewStream(ctx, rpc, enc)

	// Error message
	if err != nil {
		fields = append(fields, zap.Error(err))
		md.ll.Error("NewStream failed", fields...)
		return st, err
	}

	// Delay end message for when the stream is closed
	go func() {
		// Wait for stream
		<-st.Context().Done()

		// End message
		end := time.Now().UTC()
		lapse := end.Sub(start)
		fields = append(fields,
			zap.Duration("event.duration", lapse),
			zap.Time("event.end", end),
		)
		md.ll.Info("Stream completed", fields...)
	}()
	return st, err
}

func getFields(ctx context.Context, rpc string) []zap.Field {
	var fields []zap.Field
	segments := strings.Split(rpc, "/")
	if len(segments) == 3 {
		fields = append(fields,
			zap.String("rpc.system", "drpc"),
			zap.String("rpc.service", segments[1]),
			zap.String("rpc.method", segments[2]))
	}
	if md, ok := drpcmetadata.Get(ctx); ok {
		for k, v := range md {
			fields = append(fields, zap.String(k, v))
		}
	}
	return fields
}

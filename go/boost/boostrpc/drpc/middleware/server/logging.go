package server

import (
	"context"
	"errors"
	"io"
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
	return func(next drpc.Handler) drpc.Handler {
		return logging{
			ll:   logger,
			hook: hook,
			next: next,
		}
	}
}

// LoggingHook provides a mechanism to extend a message fields just
// before is submitted.
type LoggingHook func(stream drpc.Stream, fields []zap.Field) []zap.Field

type wrappedStream struct {
	drpc.Stream
	ll *zap.Logger
}

func (ws wrappedStream) MsgSend(msg drpc.Message, enc drpc.Encoding) (err error) {
	ws.ll.Debug("send message")
	if err = ws.Stream.MsgSend(msg, enc); err != nil {
		ws.ll.Warn("send message failed", zap.Error(err))
	}
	return
}

func (ws wrappedStream) MsgRecv(msg drpc.Message, enc drpc.Encoding) (err error) {
	ws.ll.Debug("receive message")
	if err = ws.Stream.MsgRecv(msg, enc); err != nil && !errors.Is(err, io.EOF) {
		ws.ll.Warn("receive message failed", zap.Error(err))
	}
	return
}

type logging struct {
	ll   *zap.Logger
	hook LoggingHook
	next drpc.Handler
}

func (md logging) HandleRPC(stream drpc.Stream, rpc string) error {
	// Get basic request details
	fields := getFields(stream.Context(), rpc)
	if md.hook != nil {
		fields = md.hook(stream, fields)
	}

	// Start message
	md.ll.Info(rpc, fields...)

	// Process request
	start := time.Now().UTC()
	ws := wrappedStream{
		Stream: stream,
		ll:     md.ll.With(fields...),
	}
	err := md.next.HandleRPC(ws, rpc)
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
		md.ll.Error("HandleRPC failed", fields...)
		return err
	}
	md.ll.Info("HandleRPC completed", fields...)
	return nil
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

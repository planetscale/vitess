package common

import (
	"context"

	"go.uber.org/zap"
)

func UnwrapOr[T any](v *T, defaultt T) T {
	if v == nil {
		return defaultt
	}
	return *v
}

func PtrOf[T any](v T) *T {
	return &v
}

type ctxLoggerKey struct{}

func Logger(ctx context.Context) *zap.Logger {
	return ctx.Value(ctxLoggerKey{}).(*zap.Logger)
}

func ContextWithLogger(ctx context.Context, log *zap.Logger) context.Context {
	return context.WithValue(ctx, ctxLoggerKey{}, log)
}

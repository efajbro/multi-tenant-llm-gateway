package logging

import (
	"context"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type contextKey string
const loggerKey contextKey = "logger"

var (
	globalLogger *zap.Logger
	atomicLevel  zap.AtomicLevel
)

// Init initialises the global zap logger. Call once at startup.
func Init(mode string) error {
	atomicLevel = zap.NewAtomicLevelAt(zapcore.InfoLevel)
	var cfg zap.Config
	if mode == "development" {
		cfg = zap.NewDevelopmentConfig()
	} else {
		cfg = zap.NewProductionConfig()
	}
	cfg.Level = atomicLevel
	cfg.EncoderConfig.TimeKey = "ts"
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	l, err := cfg.Build()
	if err != nil {
		return err
	}
	globalLogger = l
	return nil
}

// L returns the global logger (no-op if Init not called).
func L() *zap.Logger {
	if globalLogger == nil {
		return zap.NewNop()
	}
	return globalLogger
}

// SetLevel changes log level at runtime without restart.
func SetLevel(level zapcore.Level) { atomicLevel.SetLevel(level) }

// WithContext returns a per-request child logger stored in ctx.
func WithContext(ctx context.Context) *zap.Logger {
	if l, ok := ctx.Value(loggerKey).(*zap.Logger); ok {
		return l
	}
	return L()
}

// InjectLogger stores a child logger into the context.
func InjectLogger(ctx context.Context, fields ...zap.Field) context.Context {
	child := L().With(fields...)
	return context.WithValue(ctx, loggerKey, child)
}

// Sync flushes buffered log entries. Call on shutdown.
func Sync() {
	if globalLogger != nil {
		_ = globalLogger.Sync()
	}
}

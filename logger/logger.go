package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var zapLogger *zap.Logger

func Get() *zap.Logger {
	return zapLogger
}

func init() {
	zapConfig := zap.NewProductionConfig()
	zapConfig.EncoderConfig.TimeKey = "timestamp"
	zapConfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	zapLogger, _ = zapConfig.Build()
}

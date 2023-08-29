package logger

import "go.uber.org/zap"

var zapLogger *zap.Logger

func Get() *zap.Logger {
	return zapLogger
}

func init() {
	zapLogger, _ = zap.NewProduction()
}

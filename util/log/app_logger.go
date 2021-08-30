package log

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"go-mysql-transfer/config"
)

var _logger *zap.Logger

func Initialize(config *config.LoggerConfig, options ...zap.Option) error {
	if nil == config {
		_logger = zap.NewExample()
		return nil
	}

	zap, err := NewZapLogger(config, options...)
	if err == nil {
		_logger = zap
	}
	return err
}

func GetLogger() *zap.Logger {
	if nil == _logger {
		_logger = zap.NewExample()
	}
	return _logger
}

func GetSugar() *zap.SugaredLogger {
	return GetLogger().Sugar()
}

func Debug(msg string, fields ...zapcore.Field) {
	GetLogger().Debug(msg, fields...)
}

func Debugf(template string, args ...interface{}) {
	GetLogger().Sugar().Debugf(template, args...)
}

func Info(msg string, fields ...zapcore.Field) {
	GetLogger().Info(msg, fields...)
}

func Infof(template string, args ...interface{}) {
	GetLogger().Sugar().Infof(template, args...)
}

func Warn(msg string, fields ...zapcore.Field) {
	GetLogger().Warn(msg, fields...)
}

func Warnf(template string, args ...interface{}) {
	GetLogger().Sugar().Warnf(template, args...)
}

func Error(msg string, fields ...zapcore.Field) {
	GetLogger().Error(msg, fields...)
}

func Errorf(template string, args ...interface{}) {
	GetLogger().Sugar().Errorf(template, args...)
}

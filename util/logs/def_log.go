package logs

import (
	"io"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var _logger *zap.Logger
var _writer io.Writer

func Initialize(config *Config, options ...zap.Option) error {
	if nil == config {
		_logger = zap.NewExample()
		return nil
	}

	zap, writer, err := NewZapLogger(config, options...)
	if err == nil {
		_logger = zap
		_writer = writer
	}

	return err
}

func Logger() *zap.Logger {
	if nil == _logger {
		_logger = zap.NewExample()
	}
	return _logger
}

func Writer() io.Writer {
	if nil == _writer {
		_writer = os.Stdout
	}
	return _writer
}

func Sugar() *zap.SugaredLogger {
	if nil == _logger {
		_logger = zap.NewExample()
	}
	return _logger.Sugar()
}

func Debug(msg string, fields ...zapcore.Field) {
	if nil == _logger {
		_logger = zap.NewExample()
	}
	_logger.Debug(msg, fields...)
}

func Debugf(template string, args ...interface{}) {
	if nil == _logger {
		_logger = zap.NewExample()
	}
	_logger.Sugar().Debugf(template, args...)
}

func Info(msg string, fields ...zapcore.Field) {
	if nil == _logger {
		_logger = zap.NewExample()
	}
	_logger.Info(msg, fields...)
}

func Infof(template string, args ...interface{}) {
	if nil == _logger {
		_logger = zap.NewExample()
	}
	_logger.Sugar().Infof(template, args...)
}

func Warn(msg string, fields ...zapcore.Field) {
	if nil == _logger {
		_logger = zap.NewExample()
	}
	_logger.Warn(msg, fields...)
}

func Warnf(template string, args ...interface{}) {
	if nil == _logger {
		_logger = zap.NewExample()
	}
	_logger.Sugar().Warnf(template, args...)
}

func Error(msg string, fields ...zapcore.Field) {
	if nil == _logger {
		_logger = zap.NewExample()
	}
	_logger.Error(msg, fields...)
}

func Errorf(template string, args ...interface{}) {
	if nil == _logger {
		_logger = zap.NewExample()
	}
	_logger.Sugar().Errorf(template, args...)
}

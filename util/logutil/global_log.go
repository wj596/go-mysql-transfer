package logutil

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"

	"go-mysql-transfer/util/fileutil"
)

var _globalLogWriter io.Writer
var _globalLogger *zap.Logger

func InitGlobalLogger(config *LoggerConfig, options ...zap.Option) error {
	if nil == config {
		_globalLogger = zap.NewExample()
		return nil
	}

	if config.FileName == "" {
		config.FileName = _logFileName
	}

	logFile := filepath.Join(config.Store, config.FileName)
	if succeed := fileutil.CreateFileIfNecessary(logFile); !succeed {
		return errors.New(fmt.Sprintf("create log file : %s error", logFile))
	}
	_globalLogWriter = &lumberjack.Logger{ //定义日志分割器
		Filename:  logFile,         // 日志文件路径
		MaxSize:   config.MaxSize,  // 文件最大M字节
		MaxAge:    config.MaxAge,   // 最多保留几天
		Compress:  config.Compress, // 是否压缩
		LocalTime: true,
	}

	zap, err := NewZapLogger(config, _globalLogWriter, options...)
	if err == nil {
		_globalLogger = zap
	}

	return err
}

func GlobalLogger() *zap.Logger {
	if nil == _globalLogger {
		_globalLogger = zap.NewExample()
	}
	return _globalLogger
}

func GlobalLogWriter() io.Writer {
	if nil == _globalLogWriter {
		_globalLogWriter = os.Stdout
	}
	return _globalLogWriter
}

func GlobalSugar() *zap.SugaredLogger {
	return GlobalLogger().Sugar()
}

func Debug(msg string, fields ...zapcore.Field) {
	_globalLogger.Debug(msg, fields...)
}

func Debugf(template string, args ...interface{}) {
	_globalLogger.Sugar().Debugf(template, args...)
}

func Info(msg string, fields ...zapcore.Field) {
	_globalLogger.Info(msg, fields...)
}

func Infof(template string, args ...interface{}) {
	_globalLogger.Sugar().Infof(template, args...)
}

func Warn(msg string, fields ...zapcore.Field) {
	_globalLogger.Warn(msg, fields...)
}

func Warnf(template string, args ...interface{}) {
	_globalLogger.Sugar().Warnf(template, args...)
}

func Error(msg string, fields ...zapcore.Field) {
	_globalLogger.Error(msg, fields...)
}

func Errorf(template string, args ...interface{}) {
	_globalLogger.Sugar().Errorf(template, args...)
}

func BothInfof(template string, args ...interface{}) {
	log.Println(fmt.Sprintf(template, args...))
	if _globalLogger != nil {
		_globalLogger.Sugar().Infof(template, args...)
	}
}

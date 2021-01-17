/*
 * Copyright 2020-2021 the original author(https://github.com/wj596)
 *
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */
package logs

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"

	"go-mysql-transfer/util/files"
)

func NewZapLogger(config *Config, options ...zap.Option) (*zap.Logger, io.Writer, error) {
	if config.MaxSize <= 0 {
		config.MaxSize = _logMaxSize
	}
	if config.MaxSize <= 0 {
		config.MaxAge = _logMaxAge
	}
	if config.FileName == "" {
		config.FileName = _logFileName
	}

	if err := files.MkdirIfNecessary(config.Store); err != nil {
		return nil, nil, errors.New(fmt.Sprintf("create log store : %s", err.Error()))
	}

	logFile := filepath.Join(config.Store, config.FileName)
	if succeed := files.CreateFileIfNecessary(logFile); !succeed {
		return nil, nil, errors.New(fmt.Sprintf("create log file : %s error", logFile))
	}

	hook := lumberjack.Logger{ //定义日志分割器
		Filename:  logFile,         // 日志文件路径
		MaxSize:   config.MaxSize,  // 文件最大M字节
		MaxAge:    config.MaxAge,   // 最多保留几天
		Compress:  config.Compress, // 是否压缩
		LocalTime: true,
	}

	encoderConfig := newEncoderConfig()
	var encoder zapcore.Encoder
	if config.Encoding == _logEncodingJson {
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	} else {
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	}
	core := zapcore.NewCore(
		encoder,
		zapcore.AddSync(&hook),
		getZapLevel(config.Level),
	)
	return zap.New(core), &hook, nil
}

func getZapLevel(level string) zapcore.Level {
	var zapLevel zapcore.Level
	switch level {
	case _logLevelInfo:
		zapLevel = zap.InfoLevel
	case _logLevelWarn:
		zapLevel = zap.WarnLevel
	case _logLevelError:
		zapLevel = zap.ErrorLevel
	default:
		zapLevel = zap.DebugLevel
	}
	return zapLevel
}

func newEncoderConfig() zapcore.EncoderConfig {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
		enc.AppendString(t.Format("2006-01-02 15:04:05"))
	}
	encoderConfig.CallerKey = ""
	return encoderConfig
}

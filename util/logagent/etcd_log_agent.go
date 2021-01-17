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
package logagent

import (
	"go-mysql-transfer/util/logs"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func EtcdZapLoggerConfig() zap.Config {
	return zap.Config{
		Level:       zap.NewAtomicLevelAt(zap.InfoLevel),
		Development: false,
		Sampling: &zap.SamplingConfig{
			Initial:    100,
			Thereafter: 100,
		},
		Encoding: "console",
		// copied from "zap.NewProductionEncoderConfig" with some updates
		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:        "ts",
			LevelKey:       "level",
			NameKey:        "logger",
			CallerKey:      "caller",
			MessageKey:     "msg",
			StacktraceKey:  "stacktrace",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.LowercaseLevelEncoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},

		// Use "/dev/null" to discard all
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}
}

type EtcdLoggerAgent struct {
}

func NewEtcdLoggerAgent() *EtcdLoggerAgent {
	return &EtcdLoggerAgent{}
}

// Info logs to INFO log. Arguments are handled in the manner of fmt.Print.
func (s *EtcdLoggerAgent) Info(args ...interface{}) {
	for _, arg := range args {
		logs.Infof("%v", arg)
	}
}

// Infoln logs to INFO log. Arguments are handled in the manner of fmt.Println.
func (s *EtcdLoggerAgent) Infoln(args ...interface{}) {
	for _, arg := range args {
		logs.Infof("%v", arg)
	}
}

// Infof logs to INFO log. Arguments are handled in the manner of fmt.Printf.
func (s *EtcdLoggerAgent) Infof(format string, args ...interface{}) {
	logs.Infof(format, args...)
}

// Warning logs to WARNING log. Arguments are handled in the manner of fmt.Print.
func (s *EtcdLoggerAgent) Warning(args ...interface{}) {
	for _, arg := range args {
		logs.Warnf("%v", arg)
	}
}

// Warningln logs to WARNING log. Arguments are handled in the manner of fmt.Println.
func (s *EtcdLoggerAgent) Warningln(args ...interface{}) {
	for _, arg := range args {
		logs.Warnf("%v", arg)
	}
}

// Warningf logs to WARNING log. Arguments are handled in the manner of fmt.Printf.
func (s *EtcdLoggerAgent) Warningf(format string, args ...interface{}) {
	logs.Warnf(format, args...)
}

// Error logs to ERROR log. Arguments are handled in the manner of fmt.Print.
func (s *EtcdLoggerAgent) Error(args ...interface{}) {
	for _, arg := range args {
		logs.Errorf("%v", arg)
	}
}

// Errorln logs to ERROR log. Arguments are handled in the manner of fmt.Println.
func (s *EtcdLoggerAgent) Errorln(args ...interface{}) {
	for _, arg := range args {
		logs.Errorf("%v", arg)
	}
}

// Errorf logs to ERROR log. Arguments are handled in the manner of fmt.Printf.
func (s *EtcdLoggerAgent) Errorf(format string, args ...interface{}) {
	logs.Errorf(format, args)
}

// Fatal logs to ERROR log. Arguments are handled in the manner of fmt.Print.
// gRPC ensures that all Fatal logs will exit with os.Exit(1).
// Implementations may also call os.Exit() with a non-zero exit code.
func (s *EtcdLoggerAgent) Fatal(args ...interface{}) {
	for _, arg := range args {
		logs.Errorf("%v", arg)
	}
}

// Fatalln logs to ERROR log. Arguments are handled in the manner of fmt.Println.
// gRPC ensures that all Fatal logs will exit with os.Exit(1).
// Implementations may also call os.Exit() with a non-zero exit code.
func (s *EtcdLoggerAgent) Fatalln(args ...interface{}) {
	for _, arg := range args {
		logs.Errorf("%v", arg)
	}
}

// Fatalf logs to ERROR log. Arguments are handled in the manner of fmt.Printf.
// gRPC ensures that all Fatal logs will exit with os.Exit(1).
// Implementations may also call os.Exit() with a non-zero exit code.
func (s *EtcdLoggerAgent) Fatalf(format string, args ...interface{}) {
	logs.Errorf(format, args)
}

// V reports whether verbosity level l is at least the requested verbose level.
func (s *EtcdLoggerAgent) V(l int) bool { return false }

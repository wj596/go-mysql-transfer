package logutil

import (
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
		Infof("%v", arg)
	}
}

// Infoln logs to INFO log. Arguments are handled in the manner of fmt.Println.
func (s *EtcdLoggerAgent) Infoln(args ...interface{}) {
	for _, arg := range args {
		Infof("%v", arg)
	}
}

// Infof logs to INFO log. Arguments are handled in the manner of fmt.Printf.
func (s *EtcdLoggerAgent) Infof(format string, args ...interface{}) {
	Infof(format, args...)
}

// Warning logs to WARNING log. Arguments are handled in the manner of fmt.Print.
func (s *EtcdLoggerAgent) Warning(args ...interface{}) {
	for _, arg := range args {
		Warnf("%v", arg)
	}
}

// Warningln logs to WARNING log. Arguments are handled in the manner of fmt.Println.
func (s *EtcdLoggerAgent) Warningln(args ...interface{}) {
	for _, arg := range args {
		Warnf("%v", arg)
	}
}

// Warningf logs to WARNING log. Arguments are handled in the manner of fmt.Printf.
func (s *EtcdLoggerAgent) Warningf(format string, args ...interface{}) {
	Warnf(format, args...)
}

// Error logs to ERROR log. Arguments are handled in the manner of fmt.Print.
func (s *EtcdLoggerAgent) Error(args ...interface{}) {
	for _, arg := range args {
		Errorf("%v", arg)
	}
}

// Errorln logs to ERROR log. Arguments are handled in the manner of fmt.Println.
func (s *EtcdLoggerAgent) Errorln(args ...interface{}) {
	for _, arg := range args {
		Errorf("%v", arg)
	}
}

// Errorf logs to ERROR log. Arguments are handled in the manner of fmt.Printf.
func (s *EtcdLoggerAgent) Errorf(format string, args ...interface{}) {
	Errorf(format, args)
}

// Fatal logs to ERROR log. Arguments are handled in the manner of fmt.Print.
// gRPC ensures that all Fatal logs will exit with os.Exit(1).
// Implementations may also call os.Exit() with a non-zero exit code.
func (s *EtcdLoggerAgent) Fatal(args ...interface{}) {
	for _, arg := range args {
		Errorf("%v", arg)
	}
}

// Fatalln logs to ERROR log. Arguments are handled in the manner of fmt.Println.
// gRPC ensures that all Fatal logs will exit with os.Exit(1).
// Implementations may also call os.Exit() with a non-zero exit code.
func (s *EtcdLoggerAgent) Fatalln(args ...interface{}) {
	for _, arg := range args {
		Errorf("%v", arg)
	}
}

// Fatalf logs to ERROR log. Arguments are handled in the manner of fmt.Printf.
// gRPC ensures that all Fatal logs will exit with os.Exit(1).
// Implementations may also call os.Exit() with a non-zero exit code.
func (s *EtcdLoggerAgent) Fatalf(format string, args ...interface{}) {
	Errorf(format, args)
}

// V reports whether verbosity level l is at least the requested verbose level.
func (s *EtcdLoggerAgent) V(l int) bool { return false }

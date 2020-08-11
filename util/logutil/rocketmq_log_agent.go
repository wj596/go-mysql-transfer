package logutil

import (
	"fmt"
	"go.uber.org/zap/zapcore"
)

type RocketmqLoggerAgent struct {
}

func NewRocketmqLoggerAgent() *RocketmqLoggerAgent {
	return &RocketmqLoggerAgent{}
}

func (s *RocketmqLoggerAgent) Debug(msg string, fields map[string]interface{}) {
	zapFields := make([]zapcore.Field, 0, len(fields))
	for k, v := range fields {
		zapFields = append(zapFields, zapcore.Field{
			Key:    k,
			Type:   zapcore.StringType,
			String: fmt.Sprintf("%v", v),
		})
	}
	Debug(msg, zapFields...)
}

func (s *RocketmqLoggerAgent) Info(msg string, fields map[string]interface{}) {
	zapFields := make([]zapcore.Field, 0, len(fields))
	for k, v := range fields {
		zapFields = append(zapFields, zapcore.Field{
			Key:    k,
			Type:   zapcore.StringType,
			String: fmt.Sprintf("%v", v),
		})
	}
	Info(msg, zapFields...)
}

func (s *RocketmqLoggerAgent) Warning(msg string, fields map[string]interface{}) {
	zapFields := make([]zapcore.Field, 0, len(fields))
	for k, v := range fields {
		zapFields = append(zapFields, zapcore.Field{
			Key:    k,
			Type:   zapcore.StringType,
			String: fmt.Sprintf("%v", v),
		})
	}
	Warn(msg, zapFields...)
}

func (s *RocketmqLoggerAgent) Error(msg string, fields map[string]interface{}) {
	zapFields := make([]zapcore.Field, 0, len(fields))
	for k, v := range fields {
		zapFields = append(zapFields, zapcore.Field{
			Key:    k,
			Type:   zapcore.StringType,
			String: fmt.Sprintf("%v", v),
		})
	}
	Error(msg, zapFields...)
}

func (s *RocketmqLoggerAgent) Fatal(msg string, fields map[string]interface{}) {
	s.Error(msg, fields)
}

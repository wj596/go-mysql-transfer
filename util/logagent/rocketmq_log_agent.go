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
	"fmt"
	"go.uber.org/zap/zapcore"

	"go-mysql-transfer/util/logs"
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
	logs.Debug(msg, zapFields...)
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
	logs.Info(msg, zapFields...)
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
	logs.Warn(msg, zapFields...)
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
	logs.Error(msg, zapFields...)
}

func (s *RocketmqLoggerAgent) Fatal(msg string, fields map[string]interface{}) {
	s.Error(msg, fields)
}

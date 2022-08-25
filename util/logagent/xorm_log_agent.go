/*
 * Copyright 2021-2022 the original author(https://github.com/wj596)
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

	"xorm.io/core"

	"go-mysql-transfer/util/log"
)

type XormLoggerAgent struct {
	logLevel core.LogLevel
	showSql  bool
}

func NewXormLoggerAgent() *XormLoggerAgent {
	return &XormLoggerAgent{}
}

func (s *XormLoggerAgent) Debug(v ...interface{}) {
	if s.logLevel <= core.LOG_DEBUG {
		log.Debug(fmt.Sprint(v...))
	}
}

func (s *XormLoggerAgent) Debugf(format string, v ...interface{}) {
	if s.logLevel <= core.LOG_DEBUG {
		log.Debug(fmt.Sprintf(format, v...))
	}
}

func (s *XormLoggerAgent) Error(v ...interface{}) {
	if s.logLevel <= core.LOG_ERR {
		log.Error(fmt.Sprint(v...))
	}
}

func (s *XormLoggerAgent) Errorf(format string, v ...interface{}) {
	if s.logLevel <= core.LOG_ERR {
		log.Error(fmt.Sprintf(format, v...))
	}
}

func (s *XormLoggerAgent) Info(v ...interface{}) {
	if s.logLevel <= core.LOG_INFO {
		log.Info(fmt.Sprint(v...))
	}
}

func (s *XormLoggerAgent) Infof(format string, v ...interface{}) {
	if s.logLevel <= core.LOG_INFO {
		log.Infof(fmt.Sprintf(format, v...))
	}
}

func (s *XormLoggerAgent) Warn(v ...interface{}) {
	if s.logLevel <= core.LOG_WARNING {
		log.Warn(fmt.Sprint(v...))
	}
}

func (s *XormLoggerAgent) Warnf(format string, v ...interface{}) {
	if s.logLevel <= core.LOG_WARNING {
		log.Warnf(fmt.Sprintf(format, v...))
	}
}

func (s *XormLoggerAgent) Level() core.LogLevel {
	return s.logLevel
}

func (s *XormLoggerAgent) SetLevel(l core.LogLevel) {
	s.logLevel = l
}

func (s *XormLoggerAgent) ShowSQL(show ...bool) {
	if len(show) == 0 {
		s.showSql = true
		return
	}
	s.showSql = show[0]
}

func (s *XormLoggerAgent) IsShowSQL() bool {
	return s.showSql
}

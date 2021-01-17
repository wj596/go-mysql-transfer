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

import "go-mysql-transfer/util/logs"

type MetricsLoggerAgent struct {
}

func NewMetricsLoggerAgent() *MetricsLoggerAgent {
	return &MetricsLoggerAgent{}
}

// Info logs to INFO log. Arguments are handled in the manner of fmt.Print.
func (s *MetricsLoggerAgent) Printf(format string, v ...interface{}) {
	logs.Infof(format, v)
}

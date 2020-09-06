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
package logutil

const (
	_logFileName     = "system.log"
	_logLevelInfo    = "info"
	_logLevelWarn    = "warn"
	_logLevelError   = "error"
	_logMaxSize      = 500
	_logMaxAge       = 30
	_logEncodingJson = "json"
)

// logger 配置
type LoggerConfig struct {
	Level    string `yaml:"level"`     //日志级别 debug|info|warn|error
	Store    string `yaml:"store"`     //日志目录
	FileName string `yaml:"file_name"` //日志文件名称
	MaxSize  int    `yaml:"max_size"`  //日志文件最大M字节
	MaxAge   int    `yaml:"max_age"`   //日志文件最大存活的天数
	Compress bool   `yaml:"compress"`  //是否启用压缩
	Encoding string `yaml:"encoding"`  //日志编码 console|json
}

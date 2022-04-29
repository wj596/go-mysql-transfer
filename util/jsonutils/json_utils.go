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

package jsonutils

import (
	"encoding/json"
	"unsafe"

	"github.com/json-iterator/go"
)

// ToJson JSON二进制数组
func ToJson(v interface{}) ([]byte,error) {
	bytes, err := json.Marshal(v)
	if nil != err {
		return nil, err
	}
	return bytes, nil
}

// ToJsonByJsoniter JSON二进制数组
// Jsoniter 在key数量较少是MAP序列上有一定优势
// 参考 json_benchmark_test.go
func ToJsonByJsoniter(v interface{}) ([]byte,error) {
	bytes, err := jsoniter.Marshal(v)
	if nil != err {
		return nil, err
	}
	return bytes, nil
}

// ToJsonString JSON字符串
func ToJsonString(v interface{}) (string,error) {
	bytes, err := json.Marshal(v)
	if nil != err {
		return "", err
	}
	return *(*string)(unsafe.Pointer(&bytes)), nil
}

// ToJsonStringByJsoniter JSON字符串
// Jsoniter 在key数量较少是MAP序列上有一定优势
// 参考 json_benchmark_test.go
func ToJsonStringByJsoniter(v interface{}) (string,error) {
	bytes, err := jsoniter.Marshal(v)
	if nil != err {
		return "", err
	}
	return *(*string)(unsafe.Pointer(&bytes)), nil
}

// ToJsonIndent 转格式化JSON
func ToJsonIndent(v interface{}) (string,error) {
	bytes, err := json.MarshalIndent(v, "", "\t")
	if nil != err {
		return "", err
	}
	return string(bytes), nil
}

// ToJsonIndentByJsoniter 转格式化JSON
// Jsoniter 在key数量较少是MAP序列上有一定优势
// 参考 json_benchmark_test.go
func ToJsonIndentByJsoniter(v interface{}) (string,error) {
	bytes, err := jsoniter.MarshalIndent(v, "", "\t")
	if nil != err {
		return "", err
	}
	return string(bytes), nil
}
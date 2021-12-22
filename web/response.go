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

package web

const (
	_successCode = 0
	_errorCode   = 1
)

// Resp 响应
type Resp struct {
	Code    int         `json:"code"`
	Message string      `json:"message,omitempty"`
	Result  interface{} `json:"result,omitempty"`
}

func NewSuccessResp() *Resp {
	return &Resp{
		Code: _successCode,
	}
}

func NewErrorResp() *Resp {
	return &Resp{
		Code: _errorCode,
	}
}

func (c *Resp) SetMessage(message string) *Resp {
	c.Message = message
	return c
}

func (c *Resp) SetResult(result interface{}) *Resp {
	c.Result = result
	return c
}

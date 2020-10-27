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

package httputil

import (
	"net/http"
	"time"
)

var DefaultClient = NewClient()

// 参数
type H map[string]interface{}

// 重试条件
type RetryConditionFunc func(*http.Response) bool

// 先决条件
type httpRequisite struct {
	timeout         int
	retryCount      int
	retryInterval   int
	retryConditions []RetryConditionFunc
	headers         H
}

func newHttpRequisite() *httpRequisite {
	return &httpRequisite{
		headers: make(H),
	}
}

// 设置请求头
func (c *httpRequisite) AddHeader(name string, value interface{}) {
	c.headers[name] = value
}

// 设置请求头
func (c *httpRequisite) SetHeaders(values H) {
	c.headers = values
}

// 设置超时时间，单位为秒
func (c *httpRequisite) SetTimeout(_timeout int) {
	c.timeout = _timeout
}

// 设置重试次数
func (c *httpRequisite) SetRetryCount(_retryCount int) {
	c.retryCount = _retryCount
}

// 设置重试间隔时间，单位为秒
func (c *httpRequisite) SetRetryInterval(_retryInterval int) {
	c.retryInterval = _retryInterval
}

// 添加重试条件
func (c *httpRequisite) AddRetryCondition(_retryCondition RetryConditionFunc) {
	if _retryCondition != nil {
		c.retryConditions = append(c.retryConditions, _retryCondition)
	}
}

// 判断是否需要重试
func (c *httpRequisite) retryNecessary(res *http.Response) bool {
	for _, condition := range c.retryConditions {
		if condition(res) {
			return true
		}
	}
	return false
}

// 重构
func (c *httpRequisite) refactorIfNecessary (global *httpRequisite) {
	if c.retryCount == 0 {
		c.retryCount = global.retryCount
	}

	if c.retryInterval == 0 {
		c.retryInterval = global.retryInterval
	}

	for _, retryCondition := range global.retryConditions {
		c.retryConditions = append(c.retryConditions, retryCondition)
	}

	for k, v := range global.headers {
		if _, exist := c.headers[k]; !exist {
			c.headers[k] = v
		}
	}
}

type HttpClient struct {
	cli *http.Client
	globals *httpRequisite
}

// 创建Client
func NewClient() *HttpClient {
	return &HttpClient{
		cli: &http.Client{},
		globals: newHttpRequisite(),
	}
}

// 设置超时时间，单位为秒
func (c *HttpClient) SetTimeout(timeout int) *HttpClient {
	if timeout > 0 {
		c.globals.SetTimeout(timeout)
		c.cli.Timeout = time.Duration(timeout) * time.Second
	}
	return c
}

// 获取超时时间
func (c *HttpClient) Timeout() int {
	return c.globals.timeout
}

// 设置重试次数
func (c *HttpClient) SetRetryCount(retryCount int) *HttpClient {
	c.globals.SetRetryCount(retryCount)
	return c
}

// 获取重试次数
func (c *HttpClient) RetryCount() int {
	return c.globals.retryCount
}

// 设置重试间隔时间，单位为秒
func (c *HttpClient) SetRetryInterval(retryInterval int) *HttpClient {
	c.globals.SetRetryInterval(retryInterval)
	return c
}

// 获取重试间隔时间
func (c *HttpClient) RetryInterval() int {
	return c.globals.retryInterval
}

// 添加重试条件
func (c *HttpClient) AddRetryCondition(retryCondition RetryConditionFunc) *HttpClient {
	c.globals.AddRetryCondition(retryCondition)
	return c
}

// 设置Transport （用于确定HTTP请求的创建机制）
// 如果为空，将会使用DefaultTransport
func (c *HttpClient) SetTransport(transport http.RoundTripper) *HttpClient {
	if transport != nil {
		c.cli.Transport = transport
	}
	return c
}

// 添加请求头
func (c *HttpClient) AddHeader(key string, val string) *HttpClient {
	c.globals.AddHeader(key, val)
	return c
}

// 添加请求头
func (c *HttpClient) AddHeaders(values H) *HttpClient {
	c.globals.SetHeaders(values)
	return c
}

// Get请求
func (c *HttpClient) GET(url string) *NobodyPerform {
	return newNobodyPerform(url, http.MethodGet, c)
}

// Delete请求
func (c *HttpClient) DELETE(url string) *NobodyPerform {
	return newNobodyPerform(url, http.MethodDelete, c)
}

// Post请求
func (c *HttpClient) POST(url string) *BodyPerform {
	return newBodyPerform(url, http.MethodPost, c)
}

// Put请求
func (c *HttpClient) PUT(url string) *BodyPerform {
	return newBodyPerform(url, http.MethodPut, c)
}

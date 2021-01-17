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

package httpclient

import (
	"net/http"
	"time"

	"go.uber.org/zap"

	"go-mysql-transfer/util/logs"
)

var DefaultClient = NewClient()

type HttpClient struct {
	logger   *zap.Logger
	inner    *http.Client
	criteria *requestCriteria
}

// 创建Client
func NewClient() *HttpClient {
	return &HttpClient{
		logger:   logs.Logger(),
		inner:    &http.Client{},
		criteria: newRequestCriteria(),
	}
}

// 设置超时时间，单位为秒
func (c *HttpClient) SetTimeout(timeout int) *HttpClient {
	if timeout > 0 {
		c.criteria.SetTimeout(timeout)
		c.inner.Timeout = time.Duration(timeout) * time.Second
	}
	return c
}

func (c *HttpClient) SetLogger(logger *zap.Logger) {
	c.logger = logger
}

// 获取超时时间
func (c *HttpClient) GetTimeout() int {
	return c.criteria.timeout
}

// 设置重试次数
func (c *HttpClient) SetRetryCount(retryCount int) *HttpClient {
	c.criteria.SetRetryCount(retryCount)
	return c
}

// 获取重试次数
func (c *HttpClient) GetRetryCount() int {
	return c.criteria.retryCount
}

// 设置重试间隔时间，单位为秒
func (c *HttpClient) SetRetryInterval(retryInterval int) *HttpClient {
	c.criteria.SetRetryInterval(retryInterval)
	return c
}

// 获取重试间隔时间
func (c *HttpClient) GetRetryInterval() int {
	return c.criteria.retryInterval
}

// 添加重试条件
func (c *HttpClient) AddRetryConditionFunc(retryCondition RetryConditionFunc) *HttpClient {
	c.criteria.AddRetryConditionFunc(retryCondition)
	return c
}

// 设置Transport （用于确定HTTP请求的创建机制）
// 如果为空，将会使用DefaultTransport
func (c *HttpClient) SetTransport(transport http.RoundTripper) *HttpClient {
	if transport != nil {
		c.inner.Transport = transport
	}
	return c
}

// 添加请求头
func (c *HttpClient) AddHeader(key string, val string) *HttpClient {
	c.criteria.AddHeader(key, val)
	return c
}

// 添加请求头
func (c *HttpClient) AddHeaders(values H) *HttpClient {
	c.criteria.AddHeaders(values)
	return c
}

// Get请求
func (c *HttpClient) GET(url string) *GetOrDeleteExecutor {
	t := &GetOrDeleteExecutor{
		parameters: make(H),
	}
	t.client = c
	t.method = http.MethodGet
	t.addr = url
	t.criteria = newRequestCriteria()
	return t
}

// Delete请求
func (c *HttpClient) DELETE(url string) *GetOrDeleteExecutor {
	t := &GetOrDeleteExecutor{
		parameters: make(H),
	}
	t.client = c
	t.method = http.MethodDelete
	t.addr = url
	t.criteria = newRequestCriteria()
	return t
}

// Post请求
func (c *HttpClient) POST(url string) *PostOrPutExecutor {
	t := &PostOrPutExecutor{}
	t.client = c
	t.method = http.MethodPost
	t.addr = url
	t.criteria = newRequestCriteria()
	return t
}

// Put请求
func (c *HttpClient) PUT(url string) *PostOrPutExecutor {
	t := &PostOrPutExecutor{}
	t.client = c
	t.method = http.MethodPut
	t.addr = url
	t.criteria = newRequestCriteria()
	return t
}

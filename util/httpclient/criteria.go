package httpclient

import "net/http"

// 参数
type H map[string]interface{}

// 重试条件
type RetryConditionFunc func(*http.Response) bool

// 请求条件
type requestCriteria struct {
	timeout         int
	retryCount      int
	retryInterval   int
	retryConditions []RetryConditionFunc
	headers         H
}

func newRequestCriteria() *requestCriteria {
	return &requestCriteria{
		headers: make(H),
	}
}

// 设置请求头
func (c *requestCriteria) AddHeader(name string, value interface{}) {
	c.headers[name] = value
}

// 设置请求头
func (c *requestCriteria) AddHeaders(values H) {
	for k, v := range values {
		c.headers[k] = v
	}
}

// 设置超时时间，单位为秒
func (c *requestCriteria) SetTimeout(_timeout int) {
	c.timeout = _timeout
}

// 设置重试次数
func (c *requestCriteria) SetRetryCount(_retryCount int) {
	c.retryCount = _retryCount
}

// 设置重试间隔时间，单位为秒
func (c *requestCriteria) SetRetryInterval(_retryInterval int) {
	c.retryInterval = _retryInterval
}

// 添加重试条件
func (c *requestCriteria) AddRetryConditionFunc(_retryCondition RetryConditionFunc) {
	if _retryCondition != nil {
		c.retryConditions = append(c.retryConditions, _retryCondition)
	}
}

// 判断是否需要重试
func (c *requestCriteria) needRetry(res *http.Response) bool {
	for _, condition := range c.retryConditions {
		if condition(res) {
			return true
		}
	}
	return false
}

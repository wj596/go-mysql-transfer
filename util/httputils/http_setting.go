package httputils

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/juju/errors"

	"go-mysql-transfer/util/stringutils"
)

// RetryConditionFunc 重试条件
type RetryConditionFunc func(*http.Response) bool

// P 参数
type P map[string]interface{}

// FormFile 表单项文件
type FormFile string

type HttpSetting struct {
	method           string
	url              string
	signKey          string
	timeout          int
	retryCount       int
	retryInterval    int
	expectStatusCode int
	retryConditions  []RetryConditionFunc
	headers          map[string]interface{}
	queryParameters  url.Values
	form             P
	body             interface{}
	cookies          []*http.Cookie
	bodyBuffer       *bytes.Buffer
}

func R() *HttpSetting {
	return &HttpSetting{
		headers:         make(map[string]interface{}),
		queryParameters: make(url.Values),
	}
}

// SetTimeout 设置超时时间，单位为秒
func (c *HttpSetting) SetTimeout(timeout int) *HttpSetting {
	if timeout > 0 {
		c.timeout = timeout
	}
	return c
}

// SetRetryCount 设置重试次数
func (c *HttpSetting) SetRetryCount(retryCount int) *HttpSetting {
	c.retryCount = retryCount
	return c
}

// SetRetryInterval 设置重试间隔时间，单位为秒
func (c *HttpSetting) SetRetryInterval(retryInterval int) *HttpSetting {
	c.retryInterval = retryInterval
	return c
}

// AddRetryCondition 添加重试条件
func (c *HttpSetting) AddRetryCondition(retryCondition RetryConditionFunc) *HttpSetting {
	c.retryConditions = append(c.retryConditions, retryCondition)
	return c
}

// AddHeader 添加请求头
func (c *HttpSetting) AddHeader(key string, val string) *HttpSetting {
	c.headers[key] = val
	return c
}

// AddHeaders 添加请求头
func (c *HttpSetting) AddHeaders(values map[string]interface{}) *HttpSetting {
	for k, v := range values {
		c.headers[k] = v
	}
	return c
}

// AddQueryParameter 设置查询参数
func (c *HttpSetting) AddQueryParameter(name, value string) *HttpSetting {
	c.queryParameters.Set(name, value)
	return c
}

// AddQueryParameters 设置查询参数
func (c *HttpSetting) AddQueryParameters(params map[string]string) *HttpSetting {
	for p, v := range params {
		c.AddQueryParameter(p, v)
	}
	return c
}

// SetExpect 设置期望状态
func (c *HttpSetting) SetExpect(statusCode int) *HttpSetting {
	c.expectStatusCode = statusCode
	return c
}

// SetForm 设置Form
// 请求的contentType为: "application/x-www-form-urlencoded" 或 "multipart/form-data"
func (c *HttpSetting) SetForm(form P) *HttpSetting {
	c.form = form
	return c
}

// SetValuesAsForm 设置Form
// 请求的contentType为: "application/x-www-form-urlencoded" 或 "multipart/form-data"
func (c *HttpSetting) SetValuesAsForm(data url.Values) *HttpSetting {
	form := make(P)
	for k, v := range data {
		if len(v) == 1 {
			form[k] = v[0]
		} else {
			form[k] = v
		}
	}
	c.form = form
	return c
}

// SetJson 设置Json
// 请求的contentType为: "application/json"
func (c *HttpSetting) SetJson(body interface{}) *HttpSetting {
	c.body = body
	return c
}

func (c *HttpSetting) Get(url string) (*HttpResponse, error) {
	c.method = http.MethodGet
	c.url = url
	return c.do()
}

func (c *HttpSetting) Delete(url string) (*HttpResponse, error) {
	c.method = http.MethodDelete
	c.url = url
	return c.do()
}

func (c *HttpSetting) Post(url string) (*HttpResponse, error) {
	c.method = http.MethodPost
	c.url = url
	return c.do()
}

func (c *HttpSetting) Put(url string) (*HttpResponse, error) {
	c.method = http.MethodPut
	c.url = url
	return c.do()
}

func (c *HttpSetting) do() (*HttpResponse, error) {
	var err error
	var request *http.Request
	if c.method == http.MethodPost || c.method == http.MethodPut {
		if c.body == nil && c.form == nil {
			return nil, errors.Errorf("请求体为空")
		}
		if c.body != nil {
			request, err = newJsonRequest(c)
		}
		if c.form != nil {
			multipart := false
			values := make(url.Values)
			for k, v := range c.form {
				if _, ok := v.(FormFile); ok {
					multipart = true
					break
				}
				values.Set(k, stringutils.ToString(v))
			}
			if multipart {
				values = nil
				request, err = newMultipartFormRequest(c)
			} else {
				request, err = newFormRequest(values, c)
			}
		}
	} else {
		c.url = stringutils.UrlValuesToQueryString(c.url, c.queryParameters)
		request, err = newRequest(c)
	}
	if err != nil {
		return nil, err
	}

	for k, v := range c.headers {
		request.Header.Add(k, stringutils.ToString(v))
	}

	if "" != c.signKey {
		signRequest(c.signKey, request)
	}

	var response *http.Response
	response, err = Client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if c.expectStatusCode != 0 && response.StatusCode != c.expectStatusCode {
		return nil, errors.Errorf("Http请求失败,状态码[%d]", response.StatusCode)
	}

	var body []byte
	body, err = ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	respond := &HttpResponse{
		statusCode: response.StatusCode,
		body:       body,
		size:       len(body),
	}

	return respond, nil
}

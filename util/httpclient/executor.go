package httpclient

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"

	"go-mysql-transfer/util/stringutil"
)

const (
	_contentTypeForm = 1
	_contentTypeJson = 2
)

type FormFile string

// 执行器
type executor struct {
	client   *HttpClient
	criteria *requestCriteria

	addr         string
	method       string
	expectStatus int
}

type GetOrDeleteExecutor struct {
	executor
	parameters H
}

type PostOrPutExecutor struct {
	executor
	body        interface{}
	contentType int
}

// 全局Criteria覆盖本地Criteria
func (s *executor) overrideCriteria() {
	global := s.client.criteria
	local := s.criteria

	if local.retryCount == 0 {
		local.retryCount = global.retryCount
	}

	if local.retryInterval == 0 {
		local.retryInterval = global.retryInterval
	}

	for _, retryCondition := range global.retryConditions {
		local.retryConditions = append(local.retryConditions, retryCondition)
	}

	for k, v := range global.headers {
		if _, exist := local.headers[k]; !exist {
			local.headers[k] = v
		}
	}
}

// 执行Request
func (s *executor) execute(request *http.Request) (*http.Response, error) {
	s.overrideCriteria()

	for k, v := range s.criteria.headers {
		request.Header.Add(k, stringutil.ToString(v))
	}

	startTime := time.Now().UnixNano()
	res, err := s.client.inner.Do(request)
	latency := (time.Now().UnixNano() - startTime) / int64(time.Millisecond)

	if nil == err {
		s.client.logger.Sugar().Infof("请求成功, %s | %s | %d | %d(毫秒)", request.Method, request.URL.String(), res.StatusCode, latency)
	}

	if s.criteria.retryCount > 0 && s.criteria.needRetry(res) {
		for i := 0; i < s.criteria.retryCount; i++ {
			s.client.logger.Sugar().Infof("第%d次重试： %s | %s )", i+1, request.Method, request.URL.String())

			res, err = s.client.inner.Do(request)
			if err != nil {
				s.client.logger.Error(err.Error())
			}
			if !s.criteria.needRetry(res) || (i+1) == s.criteria.retryCount {
				return res, err
			}
			<-time.After(time.Duration(s.criteria.retryInterval) * time.Second)
		}
	}

	if s.expectStatus != 0 && s.expectStatus != res.StatusCode {
		defer res.Body.Close()
		return nil, errors.Errorf("Response status code : %d (%s)", res.StatusCode, http.StatusText(res.StatusCode))
	}

	return res, err
}

// 转换Response为string
func (s *executor) responseAsString(response *http.Response) (string, error) {
	defer response.Body.Close()

	if data, err := ioutil.ReadAll(response.Body); err == nil {
		return string(data), nil
	}

	return "", nil
}

// 转换Response为string
func (s *executor) responseAsEntity(response *http.Response) (*RespondEntity, error) {
	defer response.Body.Close()

	data, err := ioutil.ReadAll(response.Body)
	if nil != err {
		return nil, err
	}

	return &RespondEntity{
		statusCode: response.StatusCode,
		data:       data,
	}, nil
}

// 设置请求头
func (r *GetOrDeleteExecutor) AddHeader(name string, value interface{}) *GetOrDeleteExecutor {
	r.criteria.AddHeader(name, value)
	return r
}

// 设置请求头
func (r *GetOrDeleteExecutor) SetHeaders(values H) *GetOrDeleteExecutor {
	r.criteria.AddHeaders(values)
	return r
}

// 设置重试次数
func (r *GetOrDeleteExecutor) SetRetryCount(_retryCount int) *GetOrDeleteExecutor {
	r.criteria.SetRetryCount(_retryCount)
	return r
}

// 设置重试间隔时间，单位为秒
func (r *GetOrDeleteExecutor) SetRetryInterval(_retryInterval int) *GetOrDeleteExecutor {
	r.criteria.SetRetryInterval(_retryInterval)
	return r
}

// 添加重试条件
func (r *GetOrDeleteExecutor) AddRetryConditionFunc(_retryCondition RetryConditionFunc) *GetOrDeleteExecutor {
	r.criteria.AddRetryConditionFunc(_retryCondition)
	return r
}

// 设置查询参数
func (r *GetOrDeleteExecutor) AddParameter(name string, value interface{}) *GetOrDeleteExecutor {
	r.parameters[name] = value
	return r
}

// 设置查询参数
func (r *GetOrDeleteExecutor) AddParameters(values H) *GetOrDeleteExecutor {
	for k, v := range values {
		r.parameters[k] = v
	}
	return r
}

// 设置预期请求状态
func (r *GetOrDeleteExecutor) SetExpectStatus(expect int) *GetOrDeleteExecutor {
	r.expectStatus = expect
	return r
}

// 执行请求
func (r *GetOrDeleteExecutor) Do() (*http.Response, error) {
	values := make(url.Values)
	for k, v := range r.parameters {
		values.Set(k, stringutil.ToString(v))
	}

	url := stringutil.UrlValuesToQueryString(r.addr, values)
	req, err := http.NewRequest(r.method, url, nil)
	if nil != err {
		return nil, err
	}

	return r.execute(req)
}

func (r *GetOrDeleteExecutor) DoForString() (string, error) {
	res, err := r.Do()
	if nil != err {
		return "", err
	}
	return r.responseAsString(res)
}

func (r *GetOrDeleteExecutor) DoForEntity() (*RespondEntity, error) {
	res, err := r.Do()
	if nil != err {
		return nil, err
	}
	return r.responseAsEntity(res)
}

// 设置请求头
func (r *PostOrPutExecutor) AddHeader(name string, value interface{}) *PostOrPutExecutor {
	r.criteria.AddHeader(name, value)
	return r
}

// 设置请求头
func (r *PostOrPutExecutor) SetHeaders(values H) *PostOrPutExecutor {
	r.criteria.AddHeaders(values)
	return r
}

// 设置重试次数
func (r *PostOrPutExecutor) SetRetryCount(_retryCount int) *PostOrPutExecutor {
	r.criteria.SetRetryCount(_retryCount)
	return r
}

// 设置重试间隔时间，单位为秒
func (r *PostOrPutExecutor) SetRetryInterval(_retryInterval int) *PostOrPutExecutor {
	r.criteria.SetRetryInterval(_retryInterval)
	return r
}

// 添加重试条件
func (r *PostOrPutExecutor) AddRetryConditionFunc(_retryCondition RetryConditionFunc) *PostOrPutExecutor {
	r.criteria.AddRetryConditionFunc(_retryCondition)
	return r
}

// 设置预期请求状态
func (r *PostOrPutExecutor) SetExpectStatus(expect int) *PostOrPutExecutor {
	r.expectStatus = expect
	return r
}

func (r *PostOrPutExecutor) SetBodyAsForm(body H) *PostOrPutExecutor {
	r.contentType = _contentTypeForm
	r.body = body
	return r
}

// 设置请求的contentType 为：
// "application/json"
func (r *PostOrPutExecutor) SetBodyAsJson(body interface{}) *PostOrPutExecutor {
	r.contentType = _contentTypeJson
	r.body = body
	return r
}

// 执行请求
func (r *PostOrPutExecutor) Do() (*http.Response, error) {
	if _contentTypeForm == r.contentType {
		return r.doFormRequest()
	}

	var data []byte
	switch r.body.(type) {
	case string:
		ft := r.body.(string)
		data = []byte(ft)
	case []byte:
		data = r.body.([]byte)
	default:
		temp, err := json.Marshal(r.body)
		if err != nil {
			return nil, err
		}
		data = temp
	}

	req, err := http.NewRequest(r.method, r.addr, bytes.NewReader(data))
	if nil != err {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")
	return r.execute(req)
}

func (r *PostOrPutExecutor) DoForString() (string, error) {
	res, err := r.Do()
	if nil != err {
		return "", err
	}
	return r.responseAsString(res)
}

func (r *PostOrPutExecutor) DoForEntity() (*RespondEntity, error) {
	res, err := r.Do()
	if nil != err {
		return nil, err
	}
	return r.responseAsEntity(res)
}

func (r *PostOrPutExecutor) doFormRequest() (*http.Response, error) {
	data := r.body.(H)
	isMultipart := false
	values := make(url.Values)
	for k, v := range data {
		if _, ok := v.(FormFile); ok {
			isMultipart = true
			values = nil
			break
		}
		values.Set(k, stringutil.ToString(v))
	}

	if !isMultipart {
		req, err := http.NewRequest(r.method, r.addr, strings.NewReader(values.Encode()))
		if nil != err {
			return nil, err
		}
		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
		return r.execute(req)
	}

	var err error
	bodyBuffer := &bytes.Buffer{}
	bodyWriter := multipart.NewWriter(bodyBuffer)

	var closings []*os.File
	defer func() {
		for _, closing := range closings {
			closing.Close()
		}
	}()

	for k, v := range data {
		switch v.(type) {
		case FormFile:
			vv := v.(FormFile)
			var fw io.Writer
			fw, err = bodyWriter.CreateFormFile(k, string(vv))
			if err != nil {
				break
			}

			var file *os.File
			file, err = os.Open(string(vv))
			if err != nil {
				break
			}
			closings = append(closings, file)

			_, err = io.Copy(fw, file)
			if err != nil {
				break
			}
		default:
			if err := bodyWriter.WriteField(k, stringutil.ToString(v)); err != nil {
				return nil, err
			}
		}
	}

	if err != nil {
		return nil, err
	}

	bodyWriter.Close()

	var req *http.Request
	req, err = http.NewRequest(r.method, r.addr, bodyBuffer)
	if nil != err {
		return nil, err
	}

	req.Header.Add("Content-Type", bodyWriter.FormDataContentType())
	return r.execute(req)
}

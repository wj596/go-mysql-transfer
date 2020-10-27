package httputil

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"

	"go-mysql-transfer/util/logutil"
	"go-mysql-transfer/util/stringutil"
)

// 执行器
type perform struct {
	client    *HttpClient
	requisite *httpRequisite

	addr         string
	method       string
	expectStatus int
}

// 执行Request
func (s *perform) execute(request *http.Request) (*http.Response, error) {
	s.requisite.refactorIfNecessary(s.client.globals)
	for k, v := range s.requisite.headers {
		request.Header.Add(k, stringutil.ToString(v))
	}

	startTime := time.Now().UnixNano()
	res, err := s.client.cli.Do(request)
	latency := (time.Now().UnixNano() - startTime) / int64(time.Millisecond)

	if nil != err {
		logutil.GlobalLogger().Error(err.Error())
	} else {
		logutil.GlobalSugar().Infof("请求成功, %s | %s | %d | %d(毫秒)", request.Method, request.URL.String(), res.StatusCode, latency)
	}

	if s.requisite.retryCount > 0 && s.requisite.retryNecessary(res) {
		for i := 0; i < s.requisite.retryCount; i++ {
			logutil.GlobalSugar().Infof("第%d次重试： %s | %s )", i+1, request.Method, request.URL.String())

			res, err = s.client.cli.Do(request)
			if err != nil {
				logutil.GlobalLogger().Error(err.Error())
			}
			if !s.requisite.retryNecessary(res) || (i+1) == s.requisite.retryCount {
				return res, err
			}
			<-time.After(time.Duration(s.requisite.retryInterval) * time.Second)
		}
	}

	if s.expectStatus != 0 && s.expectStatus != res.StatusCode {
		defer res.Body.Close()
		return nil, errors.Errorf("Response status code : %d (%s)", res.StatusCode, http.StatusText(res.StatusCode))
	}

	return res, err
}

// 转换Response为string
func (s *perform) toString(response *http.Response) (string, error) {
	defer response.Body.Close()

	if data, err := ioutil.ReadAll(response.Body); err == nil {
		return string(data), nil
	}

	return "", nil
}

// 转换Response为string
func (s *perform) toEntity(response *http.Response) (*RespondEntity, error) {
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

type NobodyPerform struct {
	super      *perform
	parameters H
}

func newNobodyPerform(_addr, _method string, _client *HttpClient) *NobodyPerform {
	return &NobodyPerform{
		super: &perform{
			requisite: newHttpRequisite(),
			client:    _client,
			addr:      _addr,
			method:    _method,
		},
		parameters: make(H),
	}
}

// 设置请求头
func (r *NobodyPerform) AddHeader(name string, value interface{}) *NobodyPerform {
	r.super.requisite.AddHeader(name, value)
	return r
}

// 设置请求头
func (r *NobodyPerform) SetHeaders(values H) *NobodyPerform {
	r.super.requisite.SetHeaders(values)
	return r
}

// 设置重试次数
func (r *NobodyPerform) SetRetryCount(_retryCount int) *NobodyPerform {
	r.super.requisite.SetRetryCount(_retryCount)
	return r
}

// 设置重试间隔时间，单位为秒
func (r *NobodyPerform) SetRetryInterval(_retryInterval int) *NobodyPerform {
	r.super.requisite.SetRetryInterval(_retryInterval)
	return r
}

// 添加重试条件
func (r *NobodyPerform) AddRetryCondition(_retryCondition RetryConditionFunc) *NobodyPerform {
	r.super.requisite.AddRetryCondition(_retryCondition)
	return r
}

// 设置查询参数
func (r *NobodyPerform) AddParameter(name string, value interface{}) *NobodyPerform {
	r.parameters[name] = value
	return r
}

// 设置查询参数
func (r *NobodyPerform) SetParameters(values H) *NobodyPerform {
	r.parameters = values
	return r
}

// 设置预期请求状态
func (r *NobodyPerform) SetExpectStatus(expect int) *NobodyPerform {
	r.super.expectStatus = expect
	return r
}

// 执行请求
func (r *NobodyPerform) Do() (*http.Response, error) {
	values := make(url.Values)
	for k, v := range r.parameters {
		values.Set(k, stringutil.ToString(v))
	}

	url := stringutil.UrlValuesToQueryString(r.super.addr, values)
	req, err := http.NewRequest(r.super.method, url, nil)
	if nil != err {
		return nil, err
	}

	return r.super.execute(req)
}

func (r *NobodyPerform) DoForString() (string, error) {
	res, err := r.Do()
	if nil != err {
		return "", err
	}

	return r.super.toString(res)
}

func (r *NobodyPerform) DoForEntity() (*RespondEntity, error) {
	res, err := r.Do()
	if nil != err {
		return nil, err
	}

	return r.super.toEntity(res)
}

type FormFile string

type BodyPerform struct {
	super *perform
}

// Form请求
type FormBodyPerform struct {
	base *BodyPerform
	body H
}

// Json请求
type JsonBodyPerform struct {
	base *BodyPerform
	body interface{}
}

func newBodyPerform(_addr, _method string, _client *HttpClient) *BodyPerform {
	return &BodyPerform{
		super: &perform{
			requisite: newHttpRequisite(),
			client:    _client,
			addr:      _addr,
			method:    _method,
		},
	}
}

// 设置请求头
func (r *BodyPerform) AddHeader(name string, value interface{}) *BodyPerform {
	r.super.requisite.AddHeader(name, value)
	return r
}

// 设置请求头
func (r *BodyPerform) SetHeaders(values H) *BodyPerform {
	r.super.requisite.SetHeaders(values)
	return r
}

// 设置超时时间，单位为秒
func (r *BodyPerform) SetTimeout(_timeout int) *BodyPerform {
	r.super.requisite.SetTimeout(_timeout)
	return r
}

// 设置重试次数
func (r *BodyPerform) SetRetryCount(_retryCount int) *BodyPerform {
	r.super.requisite.SetRetryCount(_retryCount)
	return r
}

// 设置重试间隔时间，单位为秒
func (r *BodyPerform) SetRetryInterval(_retryInterval int) *BodyPerform {
	r.super.requisite.SetRetryInterval(_retryInterval)
	return r
}

// 添加重试条件
func (r *BodyPerform) AddRetryCondition(_retryCondition RetryConditionFunc) *BodyPerform {
	r.super.requisite.AddRetryCondition(_retryCondition)
	return r
}

// 设置预期请求状态
func (r *BodyPerform) SetExpectStatus(expect int) *BodyPerform {
	r.super.expectStatus = expect
	return r
}


// 设置Form
// 请求的contentType为: "application/x-www-form-urlencoded" 或 "multipart/form-data"
func (r *BodyPerform) SetForm(form H) *FormBodyPerform {
	fr := &FormBodyPerform{
		base: r,
		body: form,
	}
	return fr
}

// 执行请求
func (r *FormBodyPerform) Do() (*http.Response, error) {
	var multipart bool
	for _, v := range r.body {
		switch v.(type) {
		case FormFile:
			multipart = true
			break
		default:
		}
	}

	if !multipart {
		return r.do()
	} else {
		return r.doMultipart()
	}
}

func (r *FormBodyPerform) DoForString() (string, error) {
	res, err := r.Do()
	if nil != err {
		return "", err
	}

	return r.base.super.toString(res)
}

func (r *FormBodyPerform) DoForEntity() (*RespondEntity, error) {
	res, err := r.Do()
	if nil != err {
		return nil, err
	}

	return r.base.super.toEntity(res)
}

func (r *FormBodyPerform) do() (*http.Response, error) {
	values := make(url.Values)
	for k, v := range r.body {
		values.Set(k, stringutil.ToString(v))
	}

	req, err := http.NewRequest(r.base.super.method, r.base.super.addr, strings.NewReader(values.Encode()))
	if nil != err {
		return nil, err
	}

	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	return r.base.super.execute(req)
}

func (r *FormBodyPerform) doMultipart() (*http.Response, error) {
	bodyBuffer := &bytes.Buffer{}
	bodyWriter := multipart.NewWriter(bodyBuffer)

	var closings []*os.File
	defer func() {
		fmt.Println("closings")
		for _, closing := range closings {
			closing.Close()
		}
	}()

	var err error
	for k, v := range r.body {
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

	req, err := http.NewRequest(r.base.super.method, r.base.super.addr, bodyBuffer)
	if nil != err {
		return nil, err
	}

	req.Header.Add("Content-Type", bodyWriter.FormDataContentType())
	return r.base.super.execute(req)
}

// 设置Json
// 请求的contentType为: "application/json"
func (r *BodyPerform) SetJson(data interface{}) *JsonBodyPerform {
	return &JsonBodyPerform{
		base: r,
		body: data,
	}
}

// 执行请求
func (r *JsonBodyPerform) Do() (*http.Response, error) {
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

	req, err := http.NewRequest(r.base.super.method, r.base.super.addr, bytes.NewReader(data))
	if nil != err {
		return nil, err
	}

	req.Header.Add("Content-Type", "application/json")
	return r.base.super.execute(req)
}

func (r *JsonBodyPerform) DoForString() (string, error) {
	res, err := r.Do()
	if nil != err {
		return "", err
	}

	return r.base.super.toString(res)
}

func (r *JsonBodyPerform) DoForEntity() (*RespondEntity, error) {
	res, err := r.Do()
	if nil != err {
		return nil, err
	}

	return r.base.super.toEntity(res)
}

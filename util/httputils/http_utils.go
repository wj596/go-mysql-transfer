package httputils

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path"
	"time"

	"github.com/juju/errors"

	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/util/pool"
	"go-mysql-transfer/util/stringutils"
)

var (
	Client = &http.Client{Timeout: 5 * time.Second}
)

func newRequest(setting *HttpSetting) (*http.Request, error) {
	request, err := http.NewRequest(setting.method, setting.url, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Add(ContentType, ContentTypeApplicationJson)
	request.Header.Add(CacheControl, CacheControlNoCache)
	request.Header.Add(Connection, ConnectionKeepAlive)
	request.Header.Add(UserAgent, constants.ApplicationName)
	return request, nil
}

func newJsonRequest(setting *HttpSetting) (*http.Request, error) {
	d, err := json.Marshal(setting.body)
	if err != nil {
		return nil, err
	}

	setting.bodyBuffer = pool.GetBuffer()
	setting.bodyBuffer.Write(d)
	var request *http.Request
	request, err = http.NewRequest(setting.method, setting.url, setting.bodyBuffer)
	if err != nil {
		return nil, err
	}
	request.Header.Add(ContentType, ContentTypeApplicationJson)
	request.Header.Add(CacheControl, CacheControlNoCache)
	request.Header.Add(Connection, ConnectionKeepAlive)
	request.Header.Add(UserAgent, constants.ApplicationName)
	return request, nil
}

func newFormRequest(values url.Values, setting *HttpSetting) (*http.Request, error) {
	setting.bodyBuffer = pool.GetBuffer()
	setting.bodyBuffer.Write([]byte(values.Encode()))
	request, err := http.NewRequest(setting.method, setting.url, setting.bodyBuffer)
	if nil != err {
		return nil, err
	}
	request.Header.Add(ContentType, ContentTypeApplicationForm)
	request.Header.Add(CacheControl, CacheControlNoCache)
	request.Header.Add(Connection, ConnectionKeepAlive)
	request.Header.Add(UserAgent, constants.ApplicationName)
	return request, nil
}

func newMultipartFormRequest(setting *HttpSetting) (*http.Request, error) {
	setting.bodyBuffer = pool.GetBuffer()
	bodyWriter := multipart.NewWriter(setting.bodyBuffer)

	var closings []*os.File
	defer func() {
		bodyWriter.Close()
		for _, closing := range closings {
			closing.Close()
		}
	}()

	var err error
	for k, v := range setting.form {
		switch v.(type) {
		case FormFile:
			vv := v.(FormFile)
			var file *os.File
			file, err = os.Open(string(vv))
			if err != nil {
				break
			}
			closings = append(closings, file)

			var fw io.Writer
			fw, err = bodyWriter.CreateFormFile(k, path.Base(string(vv)))
			if err != nil {
				break
			}

			_, err = io.Copy(fw, file)
			if err != nil {
				break
			}
		default:
			err = bodyWriter.WriteField(k, stringutils.ToString(v))
			if err != nil {
				return nil, err
			}
		}
	}

	if err != nil {
		return nil, err
	}

	var request *http.Request
	request, err = http.NewRequest(setting.method, setting.url, setting.bodyBuffer)
	if nil != err {
		return nil, err
	}
	request.Header.Add(ContentType, bodyWriter.FormDataContentType())
	request.Header.Add(CacheControl, CacheControlNoCache)
	request.Header.Add(Connection, ConnectionKeepAlive)
	request.Header.Add(UserAgent, constants.ApplicationName)
	return request, nil
}

func signRequest(secretKey string, request *http.Request) {
	timestamp := time.Now().UnixNano() / 1e6
	sign := Sign(timestamp, secretKey)
	request.Header.Add(HeaderParamTimestamp, stringutils.ToString(timestamp))
	request.Header.Add(HeaderParamSign, sign)
}

func Get(url string, options ...Option) ([]byte, error) {
	return doForBytes(url, http.MethodGet, nil, options...)
}

func GetForString(url string, options ...Option) (string, error) {
	data, err := doForBytes(url, http.MethodGet, nil, options...)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func GetForRespond(url string, options ...Option) (*HttpResponse, error) {
	respond, err := doForEntity(url, http.MethodGet, nil, options...)
	if err != nil {
		return nil, err
	}
	return respond, nil
}

func Delete(url string, options ...Option) error {
	resp, err := do(url, http.MethodDelete, nil, options...)
	if err == nil {
		resp.Body.Close()
	}
	return err
}

func DeleteForString(url string, options ...Option) (string, error) {
	data, err := doForBytes(url, http.MethodDelete, nil, options...)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func DeleteForRespond(url string, options ...Option) (*HttpResponse, error) {
	respond, err := doForEntity(url, http.MethodDelete, nil, options...)
	if err != nil {
		return nil, err
	}
	return respond, nil
}

func Post(url string, entity interface{}, options ...Option) error {
	resp, err := do(url, http.MethodPost, entity, options...)
	if err == nil {
		resp.Body.Close()
	}
	return err
}

func PostForString(url string, entity interface{}, options ...Option) (string, error) {
	data, err := doForBytes(url, http.MethodPost, entity, options...)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func PostForRespond(url string, entity interface{}, options ...Option) (*HttpResponse, error) {
	respond, err := doForEntity(url, http.MethodPost, entity, options...)
	if err != nil {
		return nil, err
	}
	return respond, nil
}

func Put(url string, entity interface{}, options ...Option) error {
	resp, err := do(url, http.MethodPut, entity, options...)
	if err == nil {
		resp.Body.Close()
	}
	return err
}

func PutForString(url string, entity interface{}, options ...Option) (string, error) {
	data, err := doForBytes(url, http.MethodPut, entity, options...)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func PutForRespond(url string, entity interface{}, options ...Option) (*HttpResponse, error) {
	respond, err := doForEntity(url, http.MethodPut, entity, options...)
	if err != nil {
		return nil, err
	}
	return respond, nil
}

func doForBytes(url, method string, entity interface{}, options ...Option) ([]byte, error) {
	response, err := do(url, method, entity, options...)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	var body []byte
	body, err = ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func doForEntity(url, method string, entity interface{}, options ...Option) (*HttpResponse, error) {
	response, err := do(url, method, entity, options...)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

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

func do(url, method string, requestBody interface{}, options ...Option) (*http.Response, error) {
	setting := HttpSetting{
		url:    url,
		method: method,
		body:   requestBody,
	}
	if options != nil {
		for _, option := range options {
			option.apply(&setting)
		}
	}

	var err error
	var request *http.Request
	if requestBody != nil {
		request, err = newJsonRequest(&setting)
	} else {
		request, err = newRequest(&setting)
	}
	if err != nil {
		return nil, err
	}

	if requestBody != nil {
		defer pool.ReleaseBuffer(setting.bodyBuffer)
	}

	if "" != setting.signKey {
		signRequest(setting.signKey, request)
	}

	var response *http.Response
	response, err = Client.Do(request)
	if err != nil {
		return nil, err
	}

	if setting.expectStatusCode != 0 && response.StatusCode != setting.expectStatusCode {
		response.Body.Close()
		return nil, errors.Errorf("Http请求失败,状态码[%d]", response.StatusCode)
	}

	return response, nil
}

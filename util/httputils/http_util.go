package httputils

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/juju/errors"

	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/util/stringutils"
)

var (
	Client = &http.Client{Timeout: 5 * time.Second}
)

func Get(url, secretKey string) ([]byte, error) {
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Add("Content-type", "application/json;charset=UTF-8")
	request.Header.Add("Cache-Control", "no-cache")
	request.Header.Add("Connection", "Keep-Alive")
	request.Header.Add("User-Agent", "go-mysql-transfer")

	timestamp := time.Now().UnixNano() / 1e6
	sign := Sign(timestamp, secretKey)
	request.Header.Add(constants.HeaderParamTimestamp, stringutils.ToString(timestamp))
	request.Header.Add(constants.HeaderParamSign, sign)

	response, err := Client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, errors.Errorf("Http请求失败,状态码[%d]", response.StatusCode)
	}

	var body []byte
	body, err = ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func PostForString(url, secretKey string, entity interface{}) (string, error) {
	data, err := json.Marshal(entity)
	if err != nil {
		return "", err
	}

	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	request.Header.Add("Content-type", "application/json;charset=UTF-8")
	request.Header.Add("Cache-Control", "no-cache")
	request.Header.Add("Connection", "Keep-Alive")
	request.Header.Add("User-Agent", "go-mysql-transfer")

	timestamp := time.Now().UnixNano() / 1e6
	sign := Sign(timestamp, secretKey)
	request.Header.Add(constants.HeaderParamTimestamp, stringutils.ToString(timestamp))
	request.Header.Add(constants.HeaderParamSign, sign)

	response, err := Client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return "", errors.Errorf("Http请求失败,状态码[%d]", response.StatusCode)
	}

	var body []byte
	body, err = ioutil.ReadAll(response.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func Post(url, secretKey string, entity interface{}) error {
	data, err := json.Marshal(entity)
	if err != nil {
		return err
	}

	var body io.Reader
	if entity != nil {
		body = bytes.NewReader(data)
	}
	request, err := http.NewRequest(http.MethodPost, url, body)
	if err != nil {
		return err
	}
	request.Header.Add("Content-type", "application/json;charset=UTF-8")
	request.Header.Add("Cache-Control", "no-cache")
	request.Header.Add("Connection", "Keep-Alive")
	request.Header.Add("User-Agent", "go-mysql-transfer")

	timestamp := time.Now().UnixNano() / 1e6
	sign := Sign(timestamp, secretKey)
	request.Header.Add(constants.HeaderParamTimestamp, stringutils.ToString(timestamp))
	request.Header.Add(constants.HeaderParamSign, sign)

	response, err := Client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return errors.Errorf("Http请求失败,状态码[%d]", response.StatusCode)
	}

	return nil
}

func Put(url, secretKey string, entity interface{}) error {
	data, err := json.Marshal(entity)
	if err != nil {
		return err
	}

	var body io.Reader
	if entity != nil {
		body = bytes.NewReader(data)
	}
	request, err := http.NewRequest(http.MethodPut, url, body)
	if err != nil {
		return err
	}
	request.Header.Add("Content-type", "application/json;charset=UTF-8")
	request.Header.Add("Cache-Control", "no-cache")
	request.Header.Add("Connection", "Keep-Alive")
	request.Header.Add("User-Agent", "go-mysql-transfer")

	timestamp := time.Now().UnixNano() / 1e6
	sign := Sign(timestamp, secretKey)
	request.Header.Add(constants.HeaderParamTimestamp, stringutils.ToString(timestamp))
	request.Header.Add(constants.HeaderParamSign, sign)

	response, err := Client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return errors.Errorf("Http请求失败,状态码[%d]", response.StatusCode)
	}

	return nil
}

func PutForString(url, secretKey string, entity interface{}) (string, error) {
	data, err := json.Marshal(entity)
	if err != nil {
		return "", err
	}

	var requestBody io.Reader
	if entity != nil {
		requestBody = bytes.NewReader(data)
	}
	request, err := http.NewRequest(http.MethodPut, url, requestBody)
	if err != nil {
		return "", err
	}
	request.Header.Add("Content-type", "application/json;charset=UTF-8")
	request.Header.Add("Cache-Control", "no-cache")
	request.Header.Add("Connection", "Keep-Alive")
	request.Header.Add("User-Agent", "go-mysql-transfer")

	timestamp := time.Now().UnixNano() / 1e6
	sign := Sign(timestamp, secretKey)
	request.Header.Add(constants.HeaderParamTimestamp, stringutils.ToString(timestamp))
	request.Header.Add(constants.HeaderParamSign, sign)

	response, err := Client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return "", errors.Errorf("Http请求失败,状态码[%d]", response.StatusCode)
	}

	var body []byte
	body, err = ioutil.ReadAll(response.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

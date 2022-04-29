package httputils

import (
	"net/http"
	"time"

	"github.com/juju/errors"

	"go-mysql-transfer/util/stringutils"
)

type Heartbeat struct {
	request   *http.Request
	secretKey string
}

func NewHeartbeat(url, secretKey string) *Heartbeat {
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil
	}

	request.Header.Add("Content-type", "application/json;charset=UTF-8")
	request.Header.Add("Cache-Control", "no-cache")
	request.Header.Add("Connection", "Keep-Alive")
	request.Header.Add("User-Agent", "go-mysql-transfer")

	return &Heartbeat{
		request:   request,
		secretKey: secretKey,
	}
}

func (s *Heartbeat) Do() error {
	timestamp := time.Now().UnixNano() / 1e6
	sign := Sign(timestamp, s.secretKey)
	s.request.Header.Add(HeaderParamTimestamp, stringutils.ToString(timestamp))
	s.request.Header.Add(HeaderParamSign, sign)

	response, err := Client.Do(s.request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return errors.Errorf("Http请求失败,状态码[%d]", response.StatusCode)
	}

	return nil
}

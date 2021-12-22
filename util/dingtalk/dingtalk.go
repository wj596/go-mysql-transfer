package dingtalk

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"

	"go-mysql-transfer/util/httputils"
	"go-mysql-transfer/util/log"
)

var (
	_lock sync.Mutex
)

func Send(webhook, secretKey string, message *Message) error {
	_lock.Lock()
	defer _lock.Unlock()

	values := url.Values{}
	if secretKey != "" {
		t := time.Now().UnixNano() / 1e6
		values.Set("timestamp", fmt.Sprintf("%d", t))
		values.Set("sign", httputils.Sign(t, secretKey))
	}

	var url string
	if strings.Contains(webhook, "?") {
		url = webhook + "&" + values.Encode()
	} else {
		url = webhook + "?" + values.Encode()
	}

	msgBody, err := message.GetBody()
	if err != nil {
		return err
	}

	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(msgBody))
	if err != nil {
		return err
	}
	request.Header.Add("Content-type", "application/json;charset=UTF-8")
	request.Header.Add("Cache-Control", "no-cache")
	request.Header.Add("Connection", "Keep-Alive")
	request.Header.Add("User-Agent", "go-mysql-transfer")
	response, err := httputils.Client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return errors.Errorf("Http请求失败,状态码[%d]", response.StatusCode)
	}

	respBody, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}

	var result Result
	err = json.Unmarshal(respBody, &result)
	if err != nil {
		return err
	}

	if result.Errcode != 0 {
		return errors.Errorf("发送钉钉消息失败[%s]", result.Errmsg)
	}

	log.Infof("钉钉发送成功, 消息[%s],响应[%s]", string(msgBody), string(respBody))

	return nil
}

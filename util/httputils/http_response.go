package httputils

import (
	"bytes"
	"encoding/json"
	"go-mysql-transfer/util/byteutils"
	"net/http"
)

type HttpResponse struct {
	statusCode int
	body       []byte
	size       int
}

func (r *HttpResponse) IsSucceed() bool {
	return http.StatusOK == r.statusCode && r.body != nil
}

func (r *HttpResponse) StatusCode() int {
	return r.statusCode
}

func (r *HttpResponse) Body() []byte {
	if r.body == nil {
		return []byte{}
	}
	return r.body
}

func (r *HttpResponse) ToString() string {
	if r.body != nil {
		return byteutils.BytesToString(r.body)
	}
	return ""
}

func (r *HttpResponse) ToIndentJson() string {
	if r.body == nil {
		return ""
	}
	var prettyJSON bytes.Buffer
	err := json.Indent(&prettyJSON, r.Body(), "", "\t")
	if err != nil {
		return err.Error()
	}
	return string(prettyJSON.Bytes())
}

func (r *HttpResponse) Unmarshal(entity interface{}) error {
	if r.body != nil {
		if err := json.Unmarshal(r.body, entity); err != nil {
			return err
		}
	}
	return nil
}

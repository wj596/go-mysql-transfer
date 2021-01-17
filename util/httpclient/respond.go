package httpclient

import (
	"encoding/json"
	"net/http"
)

// 应答实体
type RespondEntity struct {
	statusCode int
	data       []byte
}

func (t *RespondEntity) StatusCode() int {
	return t.statusCode
}

func (t *RespondEntity) StatusText() string {
	return http.StatusText(t.statusCode)
}

func (t *RespondEntity) Data() []byte {
	return t.data
}

func (t *RespondEntity) DataAsString() string {
	return string(t.data)
}

func (t *RespondEntity) Unmarshal(entity interface{}) error {
	err := json.Unmarshal(t.data, entity)
	if err != nil {
		return err
	}
	return nil
}

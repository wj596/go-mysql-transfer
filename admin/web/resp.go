package web

const (
	_successCode = 0
	_errorCode   = 1
)

// Resp 响应
type Resp struct {
	Code    int         `json:"code"`
	Message string      `json:"message,omitempty"`
	Result  interface{} `json:"result,omitempty"`
}

func NewSuccessResp() *Resp {
	return &Resp{
		Code: _successCode,
	}
}

func NewErrorResp() *Resp {
	return &Resp{
		Code: _errorCode,
	}
}

func (c *Resp) SetMessage(message string) *Resp {
	c.Message = message
	return c
}

func (c *Resp) SetResult(result interface{}) *Resp {
	c.Result = result
	return c
}

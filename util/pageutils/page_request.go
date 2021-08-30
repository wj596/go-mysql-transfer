package pageutils

import "go-mysql-transfer/util/stringutils"

// PageRequest 分页请求
type PageRequest struct {
	limit   int // 每页显示条数，默认 10
	current int // 当前页
}

func NewPageRequest(current int, limit int) *PageRequest {
	if current == 0 {
		current = 1
	}
	if limit == 0 {
		limit = 20
	}
	return &PageRequest{
		current: current,
		limit:   limit,
	}
}

func (c *PageRequest) Current() int {
	return c.current
}

func (c *PageRequest) Limit() int {
	return c.limit
}

func (c *PageRequest) StartIndex() int {
	return (c.current - 1) * c.limit
}

func (c *PageRequest) SetCurrent(current int) *PageRequest {
	c.current = current
	return c
}

func (c *PageRequest) SetLimit(limit int) *PageRequest {
	c.limit = limit
	return c
}

func (c *PageRequest) SetCurrentParam(current string) *PageRequest {
	c.current = stringutils.ToIntSafe(current)
	return c
}

func (c *PageRequest) SetLimitParam(limit string) *PageRequest {
	c.limit = stringutils.ToIntSafe(limit)
	return c
}

func (c *PageRequest) Necessary() bool {
	return c.limit > 0
}

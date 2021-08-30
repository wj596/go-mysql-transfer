package vo

import (
	"go-mysql-transfer/model/po"
	"go-mysql-transfer/util/pageutils"
)

type EndpointInfoParams struct {
	page *pageutils.PageRequest
	Name string
	Host string
}

type EndpointInfoResp struct {
	Total int                `json:"total"` // 总条数
	Items []*po.EndpointInfo `json:"items"` // 查询结果
}

func NewEndpointInfoParams() *EndpointInfoParams {
	return &EndpointInfoParams{
		page: new(pageutils.PageRequest),
	}
}

func (s *EndpointInfoParams) SetName(name string) *EndpointInfoParams {
	s.Name = name
	return s
}

func (s *EndpointInfoParams) SetHost(host string) *EndpointInfoParams {
	s.Host = host
	return s
}

func (s *EndpointInfoParams) Page() *pageutils.PageRequest {
	return s.page
}

func NewEndpointInfoResp() *EndpointInfoResp {
	return &EndpointInfoResp{
	}
}

func (s *EndpointInfoResp) SetTotal(total int) *EndpointInfoResp {
	s.Total = total
	return s
}

func (s *EndpointInfoResp) SetItems(items []*po.EndpointInfo) *EndpointInfoResp {
	s.Items = items
	return s
}

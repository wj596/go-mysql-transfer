package vo

import (
	"go-mysql-transfer/model/po"
	"go-mysql-transfer/util/pageutils"
)

type SourceInfoParams struct {
	page *pageutils.PageRequest
	Name string
	Host string
}

type SourceInfoResp struct {
	Total int              `json:"total"` // 总条数
	Items []*po.SourceInfo `json:"items"` // 查询结果
}

func NewSourceInfoParams() *SourceInfoParams {
	return &SourceInfoParams{
		page: new(pageutils.PageRequest),
	}
}

func (s *SourceInfoParams) SetName(name string) *SourceInfoParams {
	s.Name = name
	return s
}

func (s *SourceInfoParams) SetHost(host string) *SourceInfoParams {
	s.Host = host
	return s
}

func (s *SourceInfoParams) Page() *pageutils.PageRequest {
	return s.page
}

func NewSourceInfoResp() *SourceInfoResp {
	return &SourceInfoResp{}
}

func (s *SourceInfoResp) SetTotal(total int) *SourceInfoResp {
	s.Total = total
	return s
}

func (s *SourceInfoResp) SetItems(items []*po.SourceInfo) *SourceInfoResp {
	s.Items = items
	return s
}

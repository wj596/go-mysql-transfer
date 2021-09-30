package vo

import (
	"go-mysql-transfer/util/pageutils"
)

type PipelineInfoParams struct {
	page       *pageutils.PageRequest
	Name       string
	SourceId   uint64
	EndpointId uint64
}

type PipelineInfoResp struct {
	Total int               `json:"total"` // 总条数
	Items []*PipelineInfoVO `json:"items"` // 查询结果
}

func NewPipelineInfoParams() *PipelineInfoParams {
	return &PipelineInfoParams{
		page: new(pageutils.PageRequest),
	}
}

func (s *PipelineInfoParams) SetName(name string) *PipelineInfoParams {
	s.Name = name
	return s
}

func (s *PipelineInfoParams) SetSourceID(sourceId uint64) *PipelineInfoParams {
	s.SourceId = sourceId
	return s
}

func (s *PipelineInfoParams) SetEndpointID(endpointId uint64) *PipelineInfoParams {
	s.EndpointId = endpointId
	return s
}

func (s *PipelineInfoParams) Page() *pageutils.PageRequest {
	return s.page
}

func NewPipelineInfoResp() *PipelineInfoResp {
	return &PipelineInfoResp{}
}

func (s *PipelineInfoResp) SetTotal(total int) *PipelineInfoResp {
	s.Total = total
	return s
}

func (s *PipelineInfoResp) SetItems(items []*PipelineInfoVO) *PipelineInfoResp {
	s.Items = items
	return s
}

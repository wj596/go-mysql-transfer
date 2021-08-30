package vo

import (
	"go-mysql-transfer/model/po"
)

// PipelineInfoVO '通道信息'值对象
// see PipelineInfo
type PipelineInfoVO struct {
	Id           uint64             `json:"id,string"`
	Name         string             `json:"name"`
	SourceId     uint64             `json:"sourceId,string"`
	EndpointId   uint64             `json:"endpointId,string"`
	SourceName   string             `json:"sourceName"`
	EndpointName string             `json:"endpointName"`
	CreateTime   string             `json:"createTime"`
	UpdateTime   string             `json:"updateTime"`
	Status       uint32             `json:"status,string"`
	Rules        []*TransformRuleVO `json:"rules"`
}

func (s *PipelineInfoVO) ToPO() *po.PipelineInfo {
	p := &po.PipelineInfo{
		Id:         s.Id,
		Name:       s.Name,
		SourceId:   s.SourceId,
		EndpointId: s.EndpointId,
		CreateTime: s.CreateTime,
		Status:     s.Status,
	}
	return p
}

func (s *PipelineInfoVO) FromPO(p *po.PipelineInfo) {
	s.Id = p.Id
	s.Name = p.Name
	s.SourceId = p.SourceId
	s.EndpointId = p.EndpointId
	s.CreateTime = p.CreateTime
	s.UpdateTime = p.UpdateTime
	s.Status = p.Status
}

package service

import (
	"go-mysql-transfer/dao"
	"go-mysql-transfer/domain/po"
)

type TransformRuleService struct {
	dao         dao.TransformRuleDao
	pipelineDao dao.PipelineInfoDao
}

func (s *TransformRuleService) Get(id uint64) (*po.TransformRule, error) {
	return s.dao.Get(id)
}

func (s *TransformRuleService) SelectList(pipelineId uint64, endpointType int32) ([]*po.TransformRule, error) {
	return s.dao.SelectList(pipelineId, endpointType)
}

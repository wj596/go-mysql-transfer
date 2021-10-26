package service

import (
	"go-mysql-transfer/dao"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/domain/vo"
)

type TransformRuleService struct {
	dao         *dao.TransformRuleDao
	pipelineDao *dao.PipelineInfoDao
}

func (s *TransformRuleService) Get(id uint64) (*po.TransformRule, error) {
	return s.dao.Get(id)
}

func (s *TransformRuleService) SelectList(params vo.TransformRuleParams) ([]*po.TransformRule, error) {
	return s.dao.SelectList(params)
}

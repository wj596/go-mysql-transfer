package service

import (
	"go-mysql-transfer/dao"
	"go-mysql-transfer/model/vo"
)

type TransformRuleService struct {
	dao dao.TransformRuleDao
}

func (s *TransformRuleService) SelectList(pipelineId uint64) ([]*vo.TransformRuleVO, error) {
	ls, err := s.dao.SelectList(pipelineId)
	if err != nil {
		return nil, err
	}

	results := make([]*vo.TransformRuleVO, len(ls))
	for i, v := range ls {
		temp := new(vo.TransformRuleVO)
		temp.FromPO(v)
		results[i] = temp
	}

	return results, nil
}

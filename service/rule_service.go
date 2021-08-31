package service

import (
	"fmt"
	"go-mysql-transfer/dao"
	"go-mysql-transfer/model/vo"
	"go-mysql-transfer/util/stringutils"
)

type TransformRuleService struct {
	dao         dao.TransformRuleDao
	pipelineDao dao.PipelineInfoDao
}

func (s *TransformRuleService) Get(id uint64) (*vo.TransformRuleVO, error) {
	entity, err := s.dao.Get(id)
	if err != nil {
		return nil, err
	}

	temp := new(vo.TransformRuleVO)
	temp.FromPO(entity)
	return temp, nil
}

func (s *TransformRuleService) SelectList(pipelineId uint64, endpointType int32, isCascadePipeline bool) ([]*vo.TransformRuleVO, error) {
	ls, err := s.dao.SelectList(pipelineId, endpointType)
	if err != nil {
		return nil, err
	}

	results := make([]*vo.TransformRuleVO, len(ls))
	for i, v := range ls {
		temp := new(vo.TransformRuleVO)
		temp.FromPO(v)
		if isCascadePipeline {
			if vv, err := s.pipelineDao.Get(v.PipelineInfoId); err == nil {
				fmt.Println(stringutils.ToJsonIndent(vv))
				temp.PipelineInfoName = vv.Name
			}
		}
		results[i] = temp
	}

	return results, nil
}

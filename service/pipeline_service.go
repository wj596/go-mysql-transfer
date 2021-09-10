package service

import (
	"fmt"

	"go-mysql-transfer/config"
	"go-mysql-transfer/dao"
	"go-mysql-transfer/model/po"
	"go-mysql-transfer/model/vo"
	"go-mysql-transfer/util/dateutils"
	"go-mysql-transfer/util/snowflake"
)

type PipelineInfoService struct {
	dao         dao.PipelineInfoDao
	sourceDao   dao.SourceInfoDao
	endpointDao dao.EndpointInfoDao
	ruleDao     dao.TransformRuleDao
}

func (s *PipelineInfoService) Insert(vo *vo.PipelineInfoVO) error {
	entity := vo.ToPO()
	entity.Id, _ = snowflake.NextId()
	entity.CreateTime = dateutils.NowFormatted()
	entity.UpdateTime = dateutils.NowFormatted()
	entity.Status = config.PipelineInfoStatusInitialized

	rules := make([]*po.TransformRule, len(vo.Rules))
	for i, v := range vo.Rules {
		vv := v.ToPO()
		vv.Id, _ = snowflake.NextId()
		vv.PipelineInfoId = entity.Id
		rules[i] = vv
	}

	return s.dao.Insert(entity, rules)
}

func (s *PipelineInfoService) Update(vo *vo.PipelineInfoVO) error {
	entity := vo.ToPO()
	entity.UpdateTime = dateutils.NowFormatted()

	rules := make([]*po.TransformRule, len(vo.Rules))
	for i, v := range vo.Rules {
		vv := v.ToPO()
		vv.Id, _ = snowflake.NextId()
		vv.PipelineInfoId = entity.Id
		rules[i] = vv
	}

	return s.dao.Update(entity, rules)
}

func (s *PipelineInfoService) Delete(id uint64) error {
	return s.dao.Delete(id)
}

func (s *PipelineInfoService) Get(id uint64) (*po.PipelineInfo, error) {
	return s.dao.Get(id)
}

func (s *PipelineInfoService) GetByName(name string) (*po.PipelineInfo, error) {
	return s.dao.GetByName(name)
}

func (s *PipelineInfoService) SelectList(name string) ([]*vo.PipelineInfoVO, error) {
	items, err := s.dao.SelectList(name)
	if err != nil {
		return nil, err
	}

	for _, v := range items {
		if source, err := s.sourceDao.Get(v.SourceId); err != nil {
			return nil, err
		} else {
			v.SourceName = fmt.Sprintf("%s[%s:%d]", source.Name, source.Host, source.Port)
		}
		if endpoint, err := s.endpointDao.Get(v.EndpointId); err != nil {
			return nil, err
		} else {
			v.EndpointName = fmt.Sprintf("%s[%s %s]", endpoint.Name, config.GetTypeName(endpoint.Type), endpoint.Addresses)
		}
	}

	return items, nil
}

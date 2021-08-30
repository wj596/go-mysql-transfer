package dao

import (
	"go-mysql-transfer/config"
	"go-mysql-transfer/dao/bolt"
	"go-mysql-transfer/model/po"
	"go-mysql-transfer/model/vo"
)

type SourceInfoDao interface {
	Save(entity *po.SourceInfo) error
	Delete(id uint64) error
	Get(id uint64) (*po.SourceInfo, error)
	GetByName(name string) (*po.SourceInfo, error)
	SelectList(term *vo.SourceInfoParams) ([]*po.SourceInfo, error)
	SelectPage(term *vo.SourceInfoParams) (*vo.SourceInfoResp, error)
}

type EndpointInfoDao interface {
	Save(entity *po.EndpointInfo) error
	Delete(id uint64) error
	Get(id uint64) (*po.EndpointInfo, error)
	GetByName(name string) (*po.EndpointInfo, error)
	SelectList(term *vo.EndpointInfoParams) ([]*po.EndpointInfo, error)
	SelectPage(term *vo.EndpointInfoParams) (*vo.EndpointInfoResp, error)
}

type PipelineInfoDao interface {
	Insert(pipeline *po.PipelineInfo, rules []*po.TransformRule) error
	Update(pipeline *po.PipelineInfo, rules []*po.TransformRule) error
	Delete(id uint64) error
	Get(id uint64) (*po.PipelineInfo, error)
	GetByName(name string) (*po.PipelineInfo, error)
	SelectPage(term *vo.PipelineInfoParams) (*vo.PipelineInfoResp, error)
}

type TransformRuleDao interface {
	SelectList(pipelineId uint64) ([]*po.TransformRule, error)
}

func GetSourceInfoDao() SourceInfoDao {
	if config.GetIns().IsZkUsed() {

	}
	if config.GetIns().IsEtcdUsed() {

	}
	return new(bolt.SourceInfoDaoImpl)
}

func GetEndpointInfoDao() EndpointInfoDao {
	if config.GetIns().IsZkUsed() {

	}
	if config.GetIns().IsEtcdUsed() {

	}
	return new(bolt.EndpointInfoDaoImpl)
}

func GetPipelineInfoDao() PipelineInfoDao {
	if config.GetIns().IsZkUsed() {

	}
	if config.GetIns().IsEtcdUsed() {

	}
	return new(bolt.PipelineInfoDaoImpl)
}

func GetTransformRuleDao() TransformRuleDao {
	if config.GetIns().IsZkUsed() {

	}
	if config.GetIns().IsEtcdUsed() {

	}
	return new(bolt.TransformRuleDaoImpl)
}

package dao

import (
	"github.com/siddontang/go-mysql/mysql"
	"go-mysql-transfer/domain/vo"

	"go-mysql-transfer/config"
	"go-mysql-transfer/dao/bolt"
	"go-mysql-transfer/domain/po"
)

type ISourceInfoDao interface {
	Save(entity *po.SourceInfo) error
	Delete(id uint64) error
	Get(id uint64) (*po.SourceInfo, error)
	GetByName(name string) (*po.SourceInfo, error)
	SelectList(params *vo.SourceInfoParams) ([]*po.SourceInfo, error)
}

type IEndpointInfoDao interface {
	Save(entity *po.EndpointInfo) error
	Delete(id uint64) error
	Get(id uint64) (*po.EndpointInfo, error)
	GetByName(name string) (*po.EndpointInfo, error)
	SelectList(params *vo.EndpointInfoParams) ([]*po.EndpointInfo, error)
}

type IPipelineInfoDao interface {
	Insert(pipeline *po.PipelineInfo, rules []*po.TransformRule) error
	UpdateEntity(pipeline *po.PipelineInfo, rules []*po.TransformRule) error
	UpdateStatus(id uint64, status uint32) error
	UpdatePosition(id uint64, pos mysql.Position) error
	Delete(id uint64) error
	Get(id uint64) (*po.PipelineInfo, error)
	GetPosition(id uint64) (mysql.Position, error)
	GetByName(name string) (*po.PipelineInfo, error)
	GetBySourceAndEndpoint(sourceId, endpointId uint64) (*po.PipelineInfo, error)
	SelectList(params *vo.PipelineInfoParams) ([]*po.PipelineInfo, error)
}

type ITransformRuleDao interface {
	Get(id uint64) (*po.TransformRule, error)
	SelectList(params vo.TransformRuleParams) ([]*po.TransformRule, error)
}

func GetSourceInfoDao() ISourceInfoDao {
	if config.GetIns().IsZkUsed() {

	}
	if config.GetIns().IsEtcdUsed() {

	}
	return new(bolt.SourceInfoDao)
}

func GetEndpointInfoDao() IEndpointInfoDao {
	if config.GetIns().IsZkUsed() {

	}
	if config.GetIns().IsEtcdUsed() {

	}
	return new(bolt.EndpointInfoDao)
}

func GetPipelineInfoDao() IPipelineInfoDao {
	if config.GetIns().IsZkUsed() {

	}
	if config.GetIns().IsEtcdUsed() {

	}
	return new(bolt.PipelineInfoDao)
}

func GetTransformRuleDao() ITransformRuleDao {
	if config.GetIns().IsZkUsed() {

	}
	if config.GetIns().IsEtcdUsed() {

	}
	return new(bolt.TransformRuleDao)
}

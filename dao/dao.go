package dao

import (
	"go-mysql-transfer/config"
)

func Initialize(config *config.AppConfig) error {
	if err := initBolt(config); err != nil {
		return err
	}

	if config.IsZkUsed() {
		if err := initZookeeper(config); err != nil {
			return err
		}
	}

	if config.IsEtcdUsed() {
		if err := initEtcd(config); err != nil {
			return err
		}
	}

	return nil
}

func Close() {
	closeBolt()
	closeZookeeper()
	closeEtcd()
}

func GetSourceInfoDao() *SourceInfoDao {
	return new(SourceInfoDao)
}

func GetEndpointInfoDao() *EndpointInfoDao {
	return new(EndpointInfoDao)
}

func GetTransformRuleDao() *TransformRuleDao {
	return new(TransformRuleDao)
}

func GetPipelineInfoDao() *PipelineInfoDao {
	return new(PipelineInfoDao)
}

func GetStreamStateDao() *StreamStateDao {
	return new(StreamStateDao)
}
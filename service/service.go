package service

import (
	"go-mysql-transfer/dao"
	"go-mysql-transfer/util/snowflake"
)

var (
	_authService         *AuthService
	_sourceInfoService   *SourceInfoService
	_endpointInfoService *EndpointInfoService
	_transformRuleService *TransformRuleService
	_pipelineInfoService *PipelineInfoService
)

func Initialize() error {
	snowflake.Initialize(1)

	_authService = &AuthService{
		sessionMap: make(map[string]*Session),
	}

	_sourceInfoService = &SourceInfoService{
		dao: dao.GetSourceInfoDao(),
	}

	_endpointInfoService = &EndpointInfoService{
		dao: dao.GetEndpointInfoDao(),
	}

	_transformRuleService = &TransformRuleService{
		dao:     dao.GetTransformRuleDao(),
	}

	_pipelineInfoService = &PipelineInfoService{
		dao:     dao.GetPipelineInfoDao(),
		ruleDao: dao.GetTransformRuleDao(),
		sourceDao:   dao.GetSourceInfoDao(),
		endpointDao: dao.GetEndpointInfoDao(),
	}

	return nil
}

func GetAuthService() *AuthService {
	return _authService
}

func GetSourceInfoService() *SourceInfoService {
	return _sourceInfoService
}

func GetEndpointInfoService() *EndpointInfoService {
	return _endpointInfoService
}

func GetTransformRuleService() *TransformRuleService {
	return _transformRuleService
}

func GetPipelineInfoService() *PipelineInfoService {
	return _pipelineInfoService
}

package config

const (
	_configUninitializedTip = "Config未初始化"

	_clusterName            = "transfer"
	_dataDir                = "store"
	_prometheusExporterPort = 9595
	_webPort                = 8060
	_rpcPort                = 7060
)

const (
	EndpointTypeRedis         = 1
	EndpointTypeMongoDB       = 2
	EndpointTypeElasticsearch = 3
	EndpointTypeClickHouse    = 4
	EndpointTypeRocketMQ      = 5
	EndpointTypeKafka         = 6
	EndpointTypeRabbitMQ      = 7
	EndpointTypeHttp          = 8
	EndpointTypeGrpc          = 9
)

const (
	TransformRuleTypeRule = 0 //规则
	TransformRuleTypeLuaScript = 1 //脚本
)

const (
	EsIndexBuildTypeExtend     = "0" //使用已经存在的
	EsIndexBuildTypeAutoCreate = "1" //自动创建
)

const (
	PipelineInfoStatusInitialized   = 0 //未启动
	PipelineInfoStatusRunning = 1 //运行中
	PipelineInfoStatusPause = 2 //暂停
)






func GetTypeName(endpointType uint32) string {
	switch endpointType {
	case EndpointTypeRedis :
		return "Redis"
	case EndpointTypeMongoDB :
		return "MongoDB"
	case EndpointTypeElasticsearch :
		return "Elasticsearch"
	case EndpointTypeRocketMQ :
		return "RocketMQ"
	case EndpointTypeKafka :
		return "Kafka"
	case EndpointTypeRabbitMQ :
		return "RabbitMQ"
	case EndpointTypeHttp :
		return "Http"
	}

	return ""
}


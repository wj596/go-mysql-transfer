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
	StatusOK      = 0
	StatusDisable = 1

	EndpointTypeRedis         = 1
	EndpointTypeMongoDB       = 2
	EndpointTypeElasticsearch = 3
	EndpointTypeRocketMQ      = 4
	EndpointTypeKafka         = 5
	EndpointTypeRabbitMQ      = 6
	EndpointTypeHttp          = 7

	TransformRuleLuaScript = 1

	EsIndexBuildTypeExtend     = "0" //使用已经存在的
	EsIndexBuildTypeAutoCreate = "1" //自动创建
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


package constants

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

func GetEndpointTypeName(endpointType uint32) string {
	switch endpointType {
	case EndpointTypeRedis:
		return "Redis"
	case EndpointTypeMongoDB:
		return "MongoDB"
	case EndpointTypeElasticsearch:
		return "Elasticsearch"
	case EndpointTypeRocketMQ:
		return "RocketMQ"
	case EndpointTypeKafka:
		return "Kafka"
	case EndpointTypeRabbitMQ:
		return "RabbitMQ"
	case EndpointTypeHttp:
		return "Http"
	}
	return ""
}

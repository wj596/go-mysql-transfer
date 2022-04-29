package commons

import (
	"database/sql"
	"strconv"
	"strings"
	"time"

	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/util/dateutils"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/stringutils"
)

func GetEndpointTypeName(endpointType uint32) string {
	switch endpointType {
	case constants.EndpointTypeRedis:
		return "Redis"
	case constants.EndpointTypeMongoDB:
		return "MongoDB"
	case constants.EndpointTypeElasticsearch:
		return "Elasticsearch"
	case constants.EndpointTypeClickHouse:
		return "ClickHouse"
	case constants.EndpointTypeRocketMQ:
		return "RocketMQ"
	case constants.EndpointTypeKafka:
		return "Kafka"
	case constants.EndpointTypeRabbitMQ:
		return "RabbitMQ"
	case constants.EndpointTypeHttp:
		return "Http"
	case constants.EndpointTypeGrpc:
		return "gRPC"
	default:
		return ""
	}
}

func GetDataSourceName(username, password, host, schema string, port uint32, charset string) string {
	elements := make([]string, 0)
	elements = append(elements, username, ":", password, "@tcp(", host, ":", stringutils.ToString(port), ")/")
	elements = append(elements, schema)
	elements = append(elements, "?timeout=5s")
	if charset != "" {
		elements = append(elements, "&charset=")
		elements = append(elements, charset)
	}
	return strings.Join(elements, "")
}

func RawBytesToInterface(value sql.RawBytes, databaseType string) interface{} {
	if value == nil {
		return nil
	}

	switch databaseType {
	case "BIT":
		if string(value) == "\x01" {
			return int64(1)
		}
		return int64(0)
	case "DATETIME", "TIMESTAMP":
		vt, err := time.Parse(dateutils.DayTimeSecondFormatter, string(value))
		if err != nil || vt.IsZero() { // failed to parse date or zero date
			return nil
		}
		return vt.Format(dateutils.DayTimeSecondFormatter)
	case "DATE":
		vt, err := time.Parse(dateutils.DayFormatter, string(value))
		if err != nil || vt.IsZero() { // failed to parse date or zero date
			return nil
		}
		return vt.Format(dateutils.DayFormatter)
	case "TINYINT", "SMALLINT", "INT", "BIGINT", "YEAR":
		vv, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			log.Errorf("ConvertColumnData error[%s]", err.Error())
			return nil
		}
		return vv
	case "DECIMAL", "FLOAT", "DOUBLE":
		vv, err := strconv.ParseFloat(string(value), 64)
		if err != nil {
			log.Errorf("ConvertColumnData error[%s]", err.Error())
			return nil
		}
		return vv
	}

	return string(value)
}

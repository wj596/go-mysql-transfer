package endpoint

import (
	"go-mysql-transfer/domain/constants"
	"strings"

	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/po"
)

type Endpoint interface {
	Connect() error
	Ping() error
	Close()

	Consume([]*bo.RowEventRequest) error
	FullSync([]interface{}, *po.TransformRule) (int64, error)
}

func NewEndpoint(info *po.EndpointInfo) Endpoint {
	if info.Type == constants.EndpointTypeRedis {
		return newRedisEndpoint(info)
	}
	return nil
}

func isCluster(c *po.EndpointInfo) bool {
	addresses := strings.Split(c.Addresses, ",")
	return len(addresses) > 1
}

func getAddressList(c *po.EndpointInfo) []string {
	return strings.Split(c.Addresses, ",")
}

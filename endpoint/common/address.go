package common

import (
	"strings"

	"go-mysql-transfer/domain/po"
)

func IsCluster(c *po.EndpointInfo) bool {
	addresses := strings.Split(c.Addresses, ",")
	return len(addresses) > 1
}

func GetAddressList(c *po.EndpointInfo) []string {
	return strings.Split(c.Addresses, ",")
}

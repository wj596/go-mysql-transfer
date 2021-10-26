package endpoint

import (
	"github.com/yuin/gopher-lua"

	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/endpoint/redis"
)

type IEndpoint interface {
	Connect() error
	Ping() error
	Close()
}

type IStreamEndpoint interface {
	Connect() error
	Ping() error
	Close()
	Stream(requests []*bo.RowEventRequest) error
}

type IBatchEndpoint interface {
	Connect() error
	Ping() error
	Close()
	Batch(requests []*bo.RowEventRequest, ctx *bo.RuleContext, lvm *lua.LState) (int64, error)
}

func NewEndpoint(info *po.EndpointInfo) IEndpoint {
	if info.Type == constants.EndpointTypeRedis {
		return redis.NewEndpoint(info)
	}
	return nil
}

func NewStreamEndpoint(info *po.EndpointInfo) IStreamEndpoint {
	if info.Type == constants.EndpointTypeRedis {
		return redis.NewStreamEndpoint(info)
	}
	return nil
}

func NewBatchEndpoint(info *po.EndpointInfo) IBatchEndpoint {
	if info.Type == constants.EndpointTypeRedis {
		return redis.NewBatchEndpoint(info)
	}
	return nil
}

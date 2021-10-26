package redis

import (
	"github.com/juju/errors"
	"github.com/yuin/gopher-lua"

	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/util/log"
)

type BatchEndpoint struct {
	cli *Endpoint
}

func NewBatchEndpoint(info *po.EndpointInfo) *BatchEndpoint {
	return &BatchEndpoint{
		cli: &Endpoint{
			info: info,
		},
	}
}

func (s *BatchEndpoint) Batch(requests []*bo.RowEventRequest, ctx *bo.RuleContext, lvm *lua.LState) (int64, error) {
	if ctx.IsLuaEnable() {
		for _, request := range requests {
			err := s.cli.byLua(request, ctx, lvm)
			if err != nil {
				log.Errorf("lua 脚本执行失败 : %s ", errors.ErrorStack(err))
				return 0, constants.LuaScriptError
			}
		}
	} else {
		for _, request := range requests {
			err := s.cli.byRegular(request, ctx)
			if err != nil {
				log.Errorf(errors.ErrorStack(err))
				return 0, err
			}
		}
	}

	var counter int64
	results, err := s.cli.pipeline.Exec()
	if err != nil {
		log.Errorf(errors.ErrorStack(err))
		return 0, err
	}
	for _, result := range results {
		if result.Err() == nil {
			counter++
		}
	}

	return counter, err
}

func (s *BatchEndpoint) Connect() error {
	return s.cli.Connect()
}

func (s *BatchEndpoint) Ping() error {
	return s.cli.Ping()
}

func (s *BatchEndpoint) Close() {
	s.cli.Close()
}

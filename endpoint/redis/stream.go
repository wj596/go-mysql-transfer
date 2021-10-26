package redis

import (
	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/util/log"
)

type StreamEndpoint struct {
	cli *Endpoint
}

func NewStreamEndpoint(info *po.EndpointInfo) *StreamEndpoint {
	return &StreamEndpoint{
		cli: &Endpoint{
			info: info,
		},
	}
}

func (s *StreamEndpoint) Stream(requests []*bo.RowEventRequest) error {
	var err error
	for _, request := range requests {
		ctx := request.Context
		if ctx.GetTableColumnCount() != len(request.Data) {
			log.Warnf("[%] 表[%s]结构发生变更,忽略此条数据", ctx.GetPipelineName(), ctx.GetTableFullName())
			continue
		}

		if ctx.IsLuaEnable() {
			err = s.cli.byLua(request, ctx, ctx.GetLuaVM())
			if err != nil {
				log.Errorf("[%] Lua脚本执行错误[%s]", ctx.GetPipelineName(), err.Error)
				return constants.LuaScriptError
			}
		} else {
			err = s.cli.byRegular(request, ctx)
			if err != nil {
				return err
			}
		}
	}

	_, err = s.cli.pipeline.Exec()
	return err
}

func (s *StreamEndpoint) Connect() error {
	return s.cli.Connect()
}

func (s *StreamEndpoint) Ping() error {
	return s.cli.Ping()
}

func (s *StreamEndpoint) Close() {
	s.cli.Close()
}

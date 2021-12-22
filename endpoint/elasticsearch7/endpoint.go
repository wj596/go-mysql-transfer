/*
 * Copyright 2021-2022 the original author(https://github.com/wj596)
 *
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package elasticsearch7

import (
	"context"
	"strings"

	"github.com/juju/errors"
	"github.com/olivere/elastic/v7"
	"github.com/siddontang/go-mysql/canal"
	"github.com/yuin/gopher-lua"

	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/endpoint/luaengine"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/logagent"
	"go-mysql-transfer/util/stringutils"
)

type Endpoint struct {
	info   *po.EndpointInfo
	addrs  []string
	client *elastic.Client
}

func NewEndpoint(info *po.EndpointInfo) *Endpoint {
	return &Endpoint{
		info: info,
	}
}

func (s *Endpoint) Connect() error {
	addrs := strings.Split(s.info.GetAddresses(), stringutils.Comma)
	var options []elastic.ClientOptionFunc
	options = append(options, elastic.SetErrorLog(logagent.NewElsLoggerAgent()))
	options = append(options, elastic.SetURL(addrs...))
	if s.info.GetUsername() != "" && s.info.GetPassword() != "" {
		options = append(options, elastic.SetBasicAuth(s.info.GetUsername(), s.info.GetPassword()))
	}

	client, err := elastic.NewClient(options...)
	if err != nil {
		return err
	}

	s.addrs = addrs
	s.client = client

	return s.Ping()
}

func (s *Endpoint) Ping() error {
	for _, addr := range s.addrs {
		_, _, err := s.client.Ping(addr).Do(context.Background())
		if err == nil {
			return nil
		} else {
			log.Warnf(err.Error())
		}
	}
	return errors.New("Ping Elasticsearch 失败")
}

func (s *Endpoint) Close() {
	if s.client != nil {
		s.client.Stop()
	}
}

func (s *Endpoint) parseByRegular(request *bo.RowEventRequest, ctx *bo.RuleContext, bulkService *elastic.BulkService) error {
	value, err := ctx.EncodeValue(request)
	if err != nil {
		return err
	}
	id := ctx.GetPrimaryKeyValue(request)
	index := ctx.GetRule().GetEsIndexName()

	switch request.Action {
	case canal.InsertAction:
		req := elastic.NewBulkIndexRequest().Index(index).Id(stringutils.ToString(id)).Doc(value)
		bulkService.Add(req)
	case canal.UpdateAction:
		req := elastic.NewBulkUpdateRequest().Index(index).Id(stringutils.ToString(id)).Doc(value)
		bulkService.Add(req)
	case canal.DeleteAction:
		req := elastic.NewBulkDeleteRequest().Index(index).Id(stringutils.ToString(id))
		bulkService.Add(req)
	}
	log.Infof("管道[%s]、接收端[elasticsearch7]、事件[%s]、Index[%s]、Id[%v]", ctx.GetPipelineName(), request.Action, index, id)

	return nil
}

func (s *Endpoint) parseByLua(request *bo.RowEventRequest, ctx *bo.RuleContext, bulkService *elastic.BulkService, lvm *lua.LState) error {
	var L *lua.LState
	if lvm != nil {
		L = lvm
	} else {
		L = ctx.GetLuaVM()
	}

	event := L.NewTable()
	row := L.NewTable()
	luaengine.PaddingLTable(L, row, ctx.GetRow(request))
	L.SetTable(event, luaengine.RowKey, row)
	if canal.UpdateAction == request.Action {
		preRow := L.NewTable()
		luaengine.PaddingLTable(L, preRow, ctx.GetPreRow(request))
		L.SetTable(event, luaengine.PreRowKey, preRow)
	}
	L.SetTable(event, luaengine.ActionKey, lua.LString(request.Action))

	result := L.NewTable()
	L.SetGlobal(luaengine.GlobalVariableResult, result)

	err := L.CallByParam(lua.P{
		Fn:      L.GetGlobal(luaengine.HandleFunctionName),
		NRet:    0,
		Protect: true,
	}, event)
	if err != nil {
		log.Errorf("管道[%s]，表[%s]的Lua脚本执行错误[%s]", ctx.GetPipelineName(), ctx.GetTableFullName(), err.Error)
		return constants.LuaScriptError
	}

	result.ForEach(func(k lua.LValue, v lua.LValue) {
		combine := luaengine.LvToString(k)
		action := combine[0:6]
		index := combine[7:]
		id := luaengine.LvToString(L.GetTable(v, lua.LString("id")))
		value := luaengine.LvToString(L.GetTable(v, lua.LString("value")))

		switch action {
		case canal.InsertAction:
			req := elastic.NewBulkIndexRequest().Index(index).Id(stringutils.ToString(id)).Doc(value)
			bulkService.Add(req)
		case canal.UpdateAction:
			req := elastic.NewBulkUpdateRequest().Index(index).Id(stringutils.ToString(id)).Doc(value)
			bulkService.Add(req)
		case canal.DeleteAction:
			req := elastic.NewBulkDeleteRequest().Index(index).Id(stringutils.ToString(id))
			bulkService.Add(req)
		}
		log.Infof("管道[%s]、接收端[elasticsearch6]、事件[%s]、Index[%s]、Id[%v]", ctx.GetPipelineName(), request.Action, index, id)
	})

	return nil
}

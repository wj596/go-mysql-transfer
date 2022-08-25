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

package rabbitmq

import (
	"encoding/json"
	"strconv"
	"sync"
	"time"

	"github.com/siddontang/go-mysql/canal"
	"github.com/streadway/amqp"
	"github.com/yuin/gopher-lua"

	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/luautils"
	"go-mysql-transfer/util/netutils"
)

type Endpoint struct {
	info         *po.EndpointInfo
	connection   *amqp.Connection
	channel      *amqp.Channel
	queues       map[string]bool
	lockOfQueues sync.RWMutex
	serverUrl    string
}

func NewEndpoint(info *po.EndpointInfo) *Endpoint {
	return &Endpoint{
		info:   info,
		queues: make(map[string]bool),
	}
}

func (s *Endpoint) Connect() error {
	if s.channel != nil {
		s.channel.Close()
		s.channel = nil
	}

	if s.connection != nil {
		s.connection.Close()
		s.connection = nil
	}

	con, err := amqp.Dial(s.info.GetAddresses())
	if err != nil {
		return err
	}

	uri, _ := amqp.ParseURI(s.info.GetAddresses())
	s.serverUrl = uri.Host + ":" + strconv.Itoa(uri.Port)

	var chl *amqp.Channel
	chl, err = con.Channel()
	if err != nil {
		return err
	}

	s.connection = con
	s.channel = chl

	return nil
}

func (s *Endpoint) Ping() error {
	_, err := netutils.IsActiveTCPAddr(s.serverUrl)
	return err
}

func (s *Endpoint) Close() {
	if s.channel != nil {
		s.channel.Close()
	}
	if s.connection != nil {
		s.connection.Close()
	}
}

func (s *Endpoint) createQueueIfNecessary(name string) bool {
	s.lockOfQueues.RLock()
	_, exist := s.queues[name]
	s.lockOfQueues.RUnlock()
	if exist {
		return false
	}

	s.lockOfQueues.Lock()
	s.channel.QueueDeclare(name, false, false, false, false, nil)
	s.queues[name] = true
	s.lockOfQueues.Unlock()

	return true
}

func (s *Endpoint) parseByRegular(request *bo.RowEventRequest, ctx *bo.RuleContext) error {
	messageBody := bo.MessageBody{
		Action:    request.Action,
		Timestamp: time.Now().Unix(),
	}

	if ctx.GetRule().GetDataEncoder() == constants.DataEncoderJson {
		messageBody.Date = ctx.GetRow(request)
	} else {
		value, err := ctx.EncodeValue(request)
		if err != nil {
			return err
		}
		messageBody.Date = value
	}

	if canal.UpdateAction == request.Action && ctx.IsReservePreData() {
		messageBody.Raw = ctx.GetPreRow(request)
	}

	body, err := json.Marshal(&messageBody)
	if err != nil {
		return err
	}
	err = s.channel.Publish("", ctx.GetRule().GetMqTopic(), false, false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
		})

	log.Infof("管道[%s]、接收端[rabbitmq]、事件[%s]、Topic[String]", ctx.GetPipelineName(), request.Action, ctx.GetRule().GetMqTopic())

	return err
}

func (s *Endpoint) parseByLua(request *bo.RowEventRequest, ctx *bo.RuleContext, lvm *lua.LState) error {
	var L *lua.LState
	if lvm != nil {
		L = lvm
	} else {
		L = ctx.GetLuaVM()
	}

	event := L.NewTable()
	row := L.NewTable()
	luautils.PaddingLuaTableWithMap(L, row, ctx.GetRow(request))
	L.SetTable(event, constants.RowKey, row)
	if canal.UpdateAction == request.Action {
		preRow := L.NewTable()
		luautils.PaddingLuaTableWithMap(L, preRow, ctx.GetPreRow(request))
		L.SetTable(event, constants.PreRowKey, preRow)
	}
	L.SetTable(event, constants.ActionKey, lua.LString(request.Action))

	result := L.NewTable()
	L.SetGlobal(constants.GlobalVariableResult, result)

	err := L.CallByParam(lua.P{
		Fn:      L.GetGlobal(constants.HandleFunctionName),
		NRet:    0,
		Protect: true,
	}, event)
	if err != nil {
		log.Errorf("管道[%s]，表[%s]的Lua脚本执行错误[%s]", ctx.GetPipelineName(), ctx.GetTableFullName(), err.Error)
		return constants.LuaScriptError
	}

	result.ForEach(func(k lua.LValue, v lua.LValue) {
		combine := luautils.LvToString(k)
		topic := combine[20:]
		body := luautils.LvToByteArray(v)

		err = s.channel.Publish("", topic, false, false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        body,
			})
		if err != nil {
			return
		}
	})

	return err
}

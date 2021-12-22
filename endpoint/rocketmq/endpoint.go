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

package rocketmq

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/siddontang/go-mysql/canal"
	"github.com/yuin/gopher-lua"

	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/endpoint/luaengine"
	"go-mysql-transfer/util/log"
)

type Endpoint struct {
	info   *po.EndpointInfo
	client rocketmq.Producer
	retry  int
}

func NewEndpoint(info *po.EndpointInfo) *Endpoint {
	return &Endpoint{
		info:  info,
		retry: 2,
	}
}

func (s *Endpoint) Connect() error {
	options := make([]producer.Option, 0)
	serverList := strings.Split(s.info.GetAddresses(), ",")
	options = append(options, producer.WithNameServer(serverList))
	options = append(options, producer.WithRetry(s.retry))
	if s.info.GetGroupName() != "" {
		options = append(options, producer.WithGroupName(s.info.GetGroupName()))
	}
	if s.info.GetInstanceName() != "" {
		options = append(options, producer.WithInstanceName(s.info.GetInstanceName()))
	}
	if s.info.GetUsername() != "" && s.info.GetPassword() != "" {
		options = append(options, producer.WithCredentials(primitive.Credentials{
			AccessKey: s.info.GetUsername(),
			SecretKey: s.info.GetPassword(),
		}))
	}

	producer, _ := rocketmq.NewProducer(options...)
	s.client = producer

	return s.Ping()
}

func (s *Endpoint) Ping() error {
	ping := &primitive.Message{
		Topic: "BenchmarkTest",
		Body:  []byte("ping"),
	}
	_, err := s.client.SendSync(context.Background(), ping)
	return err
}

func (s *Endpoint) Close() {
	if s.client != nil {
		s.client.Shutdown()
	}
}

func (s *Endpoint) parseByRegular(request *bo.RowEventRequest, ctx *bo.RuleContext, messages []*primitive.Message) error {
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
	message := &primitive.Message{
		Topic: ctx.GetRule().GetMqTopic(),
		Body:  body,
	}

	log.Infof("管道[%s]、接收端[rocketmq]、事件[%s]、Topic[String]", ctx.GetPipelineName(), request.Action, ctx.GetRule().GetMqTopic())
	messages = append(messages, message)

	return nil
}

func (s *Endpoint) parseByLua(request *bo.RowEventRequest, ctx *bo.RuleContext, messages []*primitive.Message, lvm *lua.LState) error {
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
		topic := combine[20:]
		body := luaengine.LvToByteArray(v)

		message := &primitive.Message{
			Topic: topic,
			Body:  body,
		}

		log.Infof("管道[%s]、接收端[rocketmq]、事件[%s]、Topic[String]", ctx.GetPipelineName(), request.Action, topic)
		messages = append(messages, message)
	})

	return nil
}

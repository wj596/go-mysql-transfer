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

package kafka

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/yuin/gopher-lua"

	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/luautils"
)

type Endpoint struct {
	info     *po.EndpointInfo
	client   sarama.Client
	producer sarama.AsyncProducer
}

func NewEndpoint(info *po.EndpointInfo) *Endpoint {
	return &Endpoint{
		info: info,
	}
}

func (s *Endpoint) Connect() error {
	cfg := sarama.NewConfig()
	cfg.Producer.Partitioner = sarama.NewRandomPartitioner

	if s.info.GetUsername() != "" && s.info.GetPassword() != "" {
		cfg.Net.SASL.Enable = true
		cfg.Net.SASL.User = s.info.GetUsername()
		cfg.Net.SASL.Password = s.info.GetPassword()
	}

	var err error
	var client sarama.Client
	ls := strings.Split(s.info.GetAddresses(), ",")
	client, err = sarama.NewClient(ls, cfg)
	if err != nil {
		return errors.Errorf("创建Kafka客户端失败[%s]", err.Error())
	}

	var producer sarama.AsyncProducer
	producer, err = sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		return errors.Errorf("创建Kafka Producer失败[%s]", err.Error())
	}

	s.producer = producer
	s.client = client

	return s.Ping()
}

func (s *Endpoint) Ping() error {
	return s.client.RefreshMetadata()
}

func (s *Endpoint) Close() {
	if s.producer != nil {
		s.producer.Close()
	}
	if s.client != nil {
		s.client.Close()
	}
}

func (s *Endpoint) parseByRegular(request *bo.RowEventRequest, ctx *bo.RuleContext, messages []*sarama.ProducerMessage) error {
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
	message := &sarama.ProducerMessage{
		Topic: ctx.GetRule().GetMqTopic(),
		Value: sarama.ByteEncoder(body),
	}

	log.Infof("管道[%s]、接收端[kafka]、事件[%s]、Topic[String]", ctx.GetPipelineName(), request.Action, ctx.GetRule().GetMqTopic())
	messages = append(messages, message)

	return nil
}

func (s *Endpoint) parseByLua(request *bo.RowEventRequest, ctx *bo.RuleContext, messages []*sarama.ProducerMessage, lvm *lua.LState) error {
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

		message := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(body),
		}

		log.Infof("管道[%s]、接收端[kafka]、事件[%s]、Topic[String]", ctx.GetPipelineName(), request.Action, topic)
		messages = append(messages, message)
	})

	return nil
}

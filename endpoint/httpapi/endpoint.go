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

package httpapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/dgrijalva/jwt-go"
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/yuin/gopher-lua"

	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/endpoint/luaengine"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/stringutils"
)

type Endpoint struct {
	info   *po.EndpointInfo
	addrs  []string
	client *http.Client
}

func NewEndpoint(info *po.EndpointInfo) *Endpoint {
	return &Endpoint{
		info: info,
		client: &http.Client{
			Timeout: constants.HttpTimeout,
		},
	}
}

func (s *Endpoint) Connect() error {
	addrs := strings.Split(s.info.GetAddresses(), stringutils.Comma)
	s.addrs = addrs
	return s.Ping()
}

func (s *Endpoint) Ping() error {
	ping := &bo.HttpBody{
		Action:    constants.TestAction,
		Timestamp: time.Now().Unix(),
	}
	return s.doPost(ping)
}

func (s *Endpoint) Close() {
	if s.client != nil {
		s.client.CloseIdleConnections()
	}
}

func (s *Endpoint) selectAddr() string {
	return s.addrs[rand.Intn(len(s.addrs))]
}

func (s *Endpoint) doPost(body *bo.HttpBody) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}

	var request *http.Request
	request, err = http.NewRequest(http.MethodPost, s.selectAddr(), bytes.NewReader(data))
	if err != nil {
		return err
	}

	request.Header.Add("Content-Type", "application/json")

	if constants.AuthModeHttpBasic == s.info.GetAuthMode() {
		temp := s.info.GetUsername() + ":" + s.info.GetPassword()
		encoding := base64.StdEncoding.EncodeToString([]byte(temp))
		request.Header.Add("Authorization", "Basic "+encoding)
	}

	if constants.AuthModeJWT == s.info.GetAuthMode() {
		claims := jwt.StandardClaims{
			ExpiresAt: time.Now().Add(time.Second * time.Duration(s.info.GetJwtExpire())).Unix(),
			Issuer:    "go-mysql-transfer",
		}
		token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
		var signed string
		signed, err = token.SignedString([]byte(s.info.GetJwtSecretKey()))
		if err != nil {
			return err
		}
		request.Header.Add("Authorization", signed)
	}

	var response *http.Response
	response, err = s.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if 200 != response.StatusCode {
		return errors.Errorf("Response StatusCode[%d]", response.StatusCode)
	}

	return nil
}

func (s *Endpoint) parseByRegular(request *bo.RowEventRequest, ctx *bo.RuleContext) error {
	message := bo.HttpBody{
		Action:    request.Action,
		Timestamp: time.Now().Unix(),
	}

	if ctx.GetRule().GetDataEncoder() == constants.DataEncoderJson {
		message.Date = ctx.GetRow(request)
	} else {
		value, err := ctx.EncodeValue(request)
		if err != nil {
			return err
		}
		message.Date = value
	}

	if canal.UpdateAction == request.Action && ctx.IsReservePreData() {
		message.Raw = ctx.GetPreRow(request)
	}

	log.Infof("管道[%s]、接收端[http api]、事件[%s]", ctx.GetPipelineName(), request.Action)

	return s.doPost(&message)
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

		id := luaengine.LvToString(L.GetTable(v, lua.LString("id")))
		value := luaengine.LvToString(L.GetTable(v, lua.LString("value")))

		L.SetTable(data, lua.LString("headers"), headers)
		L.SetTable(data, lua.LString("value"), value)


		message := &primitive.Message{
			Topic: topic,
			Body:  body,
		}

		log.Infof("管道[%s]、接收端[rocketmq]、事件[%s]、Topic[String]", ctx.GetPipelineName(), request.Action, topic)
		messages = append(messages, message)
	})

	return nil
}

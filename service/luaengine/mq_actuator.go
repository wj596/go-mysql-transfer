/*
 * Copyright 2020-2021 the original author(https://github.com/wj596)
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
package luaengine

import (
	"github.com/siddontang/go-mysql/canal"
	"github.com/yuin/gopher-lua"

	"go-mysql-transfer/global"
	"go-mysql-transfer/model"
)

func mqModule(L *lua.LState) int {
	t := L.NewTable()
	L.SetFuncs(t, _mqModuleApi)
	L.Push(t)
	return 1
}

var _mqModuleApi = map[string]lua.LGFunction{
	"rawRow":    rawRow,
	"rawOldRow": rawOldRow,
	"rawAction": rawAction,

	"SEND": msgSend,
}

func msgSend(L *lua.LState) int {
	topic := L.CheckAny(1)
	msg := L.CheckAny(2)

	ret := L.GetGlobal(_globalRET)
	L.SetTable(ret, msg, topic)
	return 0
}

func DoMQOps(input map[string]interface{}, previous map[string]interface{}, action string, rule *global.Rule) ([]*model.MQRespond, error) {
	L := _pool.Get()
	defer _pool.Put(L)

	row := L.NewTable()
	paddingTable(L, row, input)
	ret := L.NewTable()
	L.SetGlobal(_globalRET, ret)
	L.SetGlobal(_globalROW, row)
	L.SetGlobal(_globalACT, lua.LString(action))

	if action == canal.UpdateAction {
		oldRow := L.NewTable()
		paddingTable(L, oldRow, previous)
		L.SetGlobal(_globalOLDROW, oldRow)
	}

	funcFromProto := L.NewFunctionFromProto(rule.LuaProto)
	L.Push(funcFromProto)
	err := L.PCall(0, lua.MultRet, nil)
	if err != nil {
		return nil, err
	}

	list := make([]*model.MQRespond, 0, ret.Len())
	ret.ForEach(func(k lua.LValue, v lua.LValue) {
		resp := new(model.MQRespond)
		resp.ByteArray = lvToByteArray(k)
		resp.Topic = lvToString(v)
		list = append(list, resp)
	})

	return list, nil
}

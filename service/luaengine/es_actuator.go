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
	"go-mysql-transfer/util/stringutil"
)

func esModule(L *lua.LState) int {
	t := L.NewTable()
	L.SetFuncs(t, _esModuleApi)
	L.Push(t)
	return 1
}

var _esModuleApi = map[string]lua.LGFunction{
	"rawRow":    rawRow,
	"rawAction": rawAction,

	"INSERT": esInsert,
	"UPDATE": esUpdate,
	"DELETE": esDelete,
}

func esInsert(L *lua.LState) int {
	index := L.CheckAny(1)
	id := L.CheckAny(2)
	body := L.CheckAny(3)

	data := L.NewTable()
	L.SetTable(data, lua.LString("index"), index)
	L.SetTable(data, lua.LString("action"), lua.LString(canal.InsertAction))
	L.SetTable(data, lua.LString("id"), id)
	L.SetTable(data, lua.LString("body"), body)

	ret := L.GetGlobal(_globalRET)
	L.SetTable(ret, lua.LString(stringutil.UUID()), data)
	return 0
}

func esUpdate(L *lua.LState) int {
	index := L.CheckAny(1)
	id := L.CheckAny(2)
	body := L.CheckAny(3)

	data := L.NewTable()
	L.SetTable(data, lua.LString("index"), index)
	L.SetTable(data, lua.LString("action"), lua.LString(canal.UpdateAction))
	L.SetTable(data, lua.LString("id"), id)
	L.SetTable(data, lua.LString("body"), body)

	ret := L.GetGlobal(_globalRET)
	L.SetTable(ret, lua.LString(stringutil.UUID()), data)
	return 0
}

func esDelete(L *lua.LState) int {
	index := L.CheckAny(1)
	id := L.CheckAny(2)

	data := L.NewTable()
	L.SetTable(data, lua.LString("index"), index)
	L.SetTable(data, lua.LString("action"), lua.LString(canal.DeleteAction))
	L.SetTable(data, lua.LString("id"), id)

	ret := L.GetGlobal(_globalRET)
	L.SetTable(ret, lua.LString(stringutil.UUID()), data)
	return 0
}

func DoESOps(input map[string]interface{}, action string, rule *global.Rule) ([]*model.ESRespond, error) {
	L := _pool.Get()
	defer _pool.Put(L)

	row := L.NewTable()
	paddingTable(L, row, input)
	ret := L.NewTable()
	L.SetGlobal(_globalRET, ret)
	L.SetGlobal(_globalROW, row)
	L.SetGlobal(_globalACT, lua.LString(action))

	funcFromProto := L.NewFunctionFromProto(rule.LuaProto)
	L.Push(funcFromProto)
	err := L.PCall(0, lua.MultRet, nil)
	if err != nil {
		return nil, err
	}

	responds := make([]*model.ESRespond, 0, ret.Len())
	ret.ForEach(func(k lua.LValue, v lua.LValue) {
		resp := new(model.ESRespond)
		resp.Index = lvToString(L.GetTable(v, lua.LString("index")))
		resp.Id = lvToString(L.GetTable(v, lua.LString("id")))
		resp.Action = lvToString(L.GetTable(v, lua.LString("action")))

		var data string
		body := L.GetTable(v, lua.LString("body"))
		switch body.Type() {
		case lua.LTNumber:
			data = lua.LVAsString(body)
		case lua.LTString:
			data = lua.LVAsString(body)
		case lua.LTTable:
			mm, _ := lvToMap(body)
			data = stringutil.ToJsonString(mm)
		default:
			data = stringutil.ToJsonString(body)
		}
		resp.Date = data
		responds = append(responds, resp)
	})

	return responds, nil
}

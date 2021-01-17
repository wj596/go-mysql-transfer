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
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/yuin/gopher-lua"

	"go-mysql-transfer/global"
	"go-mysql-transfer/model"
	"go-mysql-transfer/util/stringutil"
)

func mongoModule(L *lua.LState) int {
	t := L.NewTable()
	L.SetFuncs(t, _mongoModuleApi)
	L.Push(t)
	return 1
}

var _mongoModuleApi = map[string]lua.LGFunction{
	"rawRow":    rawRow,
	"rawAction": rawAction,

	"INSERT": mongoInsert,
	"UPDATE": mongoUpdate,
	"DELETE": mongoDelete,
	"UPSERT": mongoUpsert,
}

func mongoInsert(L *lua.LState) int {
	collection := L.CheckAny(1)
	table := L.CheckAny(2)

	data := L.NewTable()
	L.SetTable(data, lua.LString("collection"), collection)
	L.SetTable(data, lua.LString("action"), lua.LString(canal.InsertAction))
	L.SetTable(data, lua.LString("table"), table)

	ret := L.GetGlobal(_globalRET)
	L.SetTable(ret, lua.LString(stringutil.UUID()), data)
	return 0
}

func mongoUpdate(L *lua.LState) int {
	collection := L.CheckAny(1)
	id := L.CheckAny(2)
	table := L.CheckAny(3)

	data := L.NewTable()
	L.SetTable(data, lua.LString("collection"), collection)
	L.SetTable(data, lua.LString("action"), lua.LString(canal.UpdateAction))
	L.SetTable(data, lua.LString("id"), id)
	L.SetTable(data, lua.LString("table"), table)

	ret := L.GetGlobal(_globalRET)
	L.SetTable(ret, lua.LString(stringutil.UUID()), data)
	return 0
}

func mongoUpsert(L *lua.LState) int {
	collection := L.CheckAny(1)
	id := L.CheckAny(2)
	table := L.CheckAny(3)

	data := L.NewTable()
	L.SetTable(data, lua.LString("collection"), collection)
	L.SetTable(data, lua.LString("action"), lua.LString(global.UpsertAction))
	L.SetTable(data, lua.LString("id"), id)
	L.SetTable(data, lua.LString("table"), table)

	ret := L.GetGlobal(_globalRET)
	L.SetTable(ret, lua.LString(stringutil.UUID()), data)
	return 0
}

func mongoDelete(L *lua.LState) int {
	collection := L.CheckAny(1)
	id := L.CheckAny(2)

	data := L.NewTable()
	L.SetTable(data, lua.LString("collection"), collection)
	L.SetTable(data, lua.LString("action"), lua.LString(canal.DeleteAction))
	L.SetTable(data, lua.LString("id"), id)

	ret := L.GetGlobal(_globalRET)
	L.SetTable(ret, lua.LString(stringutil.UUID()), data)
	return 0
}

func DoMongoOps(input map[string]interface{}, action string, rule *global.Rule) ([]*model.MongoRespond, error) {
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

	asserted := true
	responds := make([]*model.MongoRespond, 0, ret.Len())
	ret.ForEach(func(k lua.LValue, v lua.LValue) {
		resp := new(model.MongoRespond)
		resp.Collection = lvToString(L.GetTable(v, lua.LString("collection")))
		resp.Action = lvToString(L.GetTable(v, lua.LString("action")))
		resp.Id = lvToInterface(L.GetTable(v, lua.LString("id")), true)
		lvTable := L.GetTable(v, lua.LString("table"))

		var table map[string]interface{}
		if action != canal.DeleteAction {
			table, asserted = lvToMap(lvTable)
			if !asserted {
				return
			}
			resp.Table = table
		}

		if action == canal.InsertAction {
			_id, ok := table["_id"]
			if !ok {
				resp.Id = stringutil.UUID()
				table["_id"] = resp.Id
			} else {
				resp.Id = _id
			}
		}

		responds = append(responds, resp)
	})

	if !asserted {
		return nil, errors.New("The parameter must be of table type")
	}

	return responds, nil
}

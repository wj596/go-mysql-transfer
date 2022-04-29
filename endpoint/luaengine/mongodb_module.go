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

package luaengine

import (
	"github.com/siddontang/go-mysql/canal"
	"github.com/yuin/gopher-lua"

	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/util/stringutils"
)

func preloadMongodbModule(L *lua.LState) {
	L.PreloadModule("mongodb", mongodbModuleLoader)
}

func mongodbModuleLoader(L *lua.LState) int {
	t := L.NewTable()
	L.SetFuncs(t, _mongodbApi)
	L.Push(t)
	return 1
}

var _mongodbApi = map[string]lua.LGFunction{
	"insert": mongodbInsert,
	"update": mongodbUpdate,
	"delete": mongodbDelete,
	"upsert": mongodbUpsert,
}

func mongodbInsert(L *lua.LState) int {
	collection := L.CheckString(1)
	value := L.CheckAny(2)
	combine := stringutils.JoinWithUnderline(nextId(), canal.InsertAction, collection)

	result := L.GetGlobal(constants.LuaGlobalVariableResult)
	L.SetTable(result, lua.LString(combine), value)
	return 0
}

func mongodbUpdate(L *lua.LState) int {
	collection := L.CheckString(1)
	id := L.CheckAny(2)
	value := L.CheckAny(3)
	combine := stringutils.JoinWithUnderline(nextId(), canal.UpdateAction, collection)

	data := L.NewTable()
	L.SetTable(data, lua.LString("id"), id)
	L.SetTable(data, lua.LString("value"), value)

	result := L.GetGlobal(constants.LuaGlobalVariableResult)
	L.SetTable(result, lua.LString(combine), data)
	return 0
}

func mongodbUpsert(L *lua.LState) int {
	collection := L.CheckString(1)
	id := L.CheckAny(2)
	value := L.CheckAny(3)
	combine := stringutils.JoinWithUnderline(nextId(), constants.UpsertAction, collection)

	data := L.NewTable()
	L.SetTable(data, lua.LString("id"), id)
	L.SetTable(data, lua.LString("value"), value)

	result := L.GetGlobal(constants.LuaGlobalVariableResult)
	L.SetTable(result, lua.LString(combine), data)
	return 0
}

func mongodbDelete(L *lua.LState) int {
	collection := L.CheckString(1)
	value := L.CheckAny(2)
	combine := stringutils.JoinWithUnderline(nextId(), canal.DeleteAction, collection)

	result := L.GetGlobal(constants.LuaGlobalVariableResult)
	L.SetTable(result, lua.LString(combine), value)
	return 0
}

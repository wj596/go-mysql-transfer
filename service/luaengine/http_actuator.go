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
	"github.com/yuin/gopher-lua"

	"go-mysql-transfer/util/logs"
)

func httpModule(L *lua.LState) int {
	t := L.NewTable()
	L.SetFuncs(t, _httpModuleApi)
	L.Push(t)
	return 1
}

var _httpModuleApi = map[string]lua.LGFunction{
	"get":    doGet,
	"delete": doDelete,
	"post":   doPost,
	"put":    doPut,
}

func doGet(L *lua.LState) int {
	ret := L.NewTable()
	paramUrl := L.CheckString(1)
	paramOps := L.CheckTable(2)

	cli := _httpClient.GET(paramUrl)
	if headers, ok := lvToMap(paramOps); ok {
		cli.SetHeaders(headers)
	}

	entity, err := cli.DoForEntity()
	if err != nil {
		logs.Error(err.Error())
		L.Push(lua.LNil)
		L.Push(lua.LString(err.Error()))
		return 2
	}

	ret.RawSet(lua.LString("status_code"), lua.LNumber(entity.StatusCode()))
	ret.RawSet(lua.LString("body"), lua.LString(string(entity.Data())))

	L.Push(ret)
	return 1
}

func doDelete(L *lua.LState) int {
	ret := L.NewTable()
	paramUrl := L.CheckString(1)
	paramOps := L.CheckTable(2)

	cli := _httpClient.DELETE(paramUrl)
	if headers, ok := lvToMap(paramOps); ok {
		cli.SetHeaders(headers)
	}

	entity, err := cli.DoForEntity()
	if err != nil {
		logs.Error(err.Error())
		L.Push(lua.LNil)
		L.Push(lua.LString(err.Error()))
		return 2
	}

	ret.RawSet(lua.LString("status_code"), lua.LNumber(entity.StatusCode()))
	ret.RawSet(lua.LString("body"), lua.LString(string(entity.Data())))

	L.Push(ret)
	return 1
}

func doPost(L *lua.LState) int {
	ret := L.NewTable()
	paramUrl := L.CheckString(1)
	paramHeaders := L.CheckTable(2)
	paramContents := L.CheckTable(3)

	cli := _httpClient.POST(paramUrl)
	if headers, ok := lvToMap(paramHeaders); ok {
		cli.SetHeaders(headers)
	}

	contents, ok := lvToMap(paramContents)
	if !ok {
		logs.Error("The argument must Table")
		L.Push(lua.LNil)
		L.Push(lua.LString("The argument must Table"))
		return 2
	}

	entity, err := cli.SetBodyAsForm(contents).DoForEntity()
	if err != nil {
		logs.Error(err.Error())
		L.Push(lua.LNil)
		L.Push(lua.LString(err.Error()))
		return 2
	}

	ret.RawSet(lua.LString("status_code"), lua.LNumber(entity.StatusCode()))
	ret.RawSet(lua.LString("body"), lua.LString(string(entity.Data())))

	L.Push(ret)
	return 1
}

func doPut(L *lua.LState) int {
	ret := L.NewTable()
	paramUrl := L.CheckString(1)
	paramHeaders := L.CheckTable(2)
	paramContents := L.CheckTable(3)

	cli := _httpClient.PUT(paramUrl)
	if headers, ok := lvToMap(paramHeaders); ok {
		cli.SetHeaders(headers)
	}

	contents, ok := lvToMap(paramContents)
	if !ok {
		logs.Error("The argument must Table")
		L.Push(lua.LNil)
		L.Push(lua.LString("The argument must Table"))
		return 2
	}

	entity, err := cli.SetBodyAsForm(contents).DoForEntity()
	if err != nil {
		logs.Error(err.Error())
		L.Push(lua.LNil)
		L.Push(lua.LString(err.Error()))
		return 2
	}

	ret.RawSet(lua.LString("status_code"), lua.LNumber(entity.StatusCode()))
	ret.RawSet(lua.LString("body"), lua.LString(string(entity.Data())))

	L.Push(ret)
	return 1
}

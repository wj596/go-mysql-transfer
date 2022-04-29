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
	"net/http"

	"github.com/yuin/gopher-lua"

	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/util/httputils"
)

var _httpClient = &http.Client{Timeout: constants.HttpTimeout}

func preloadHttpClientModule(L *lua.LState) {
	L.PreloadModule("httpclient", httpClientModuleLoader)
}

func httpClientModuleLoader(L *lua.LState) int {
	t := L.NewTable()
	L.SetFuncs(t, _httpClientApi)
	L.Push(t)
	return 1
}

var _httpClientApi = map[string]lua.LGFunction{
	"get":    doGet,
	"delete ": doDelete,
	"post":   doPost,
	"put":    doPut,
}

func doGet(L *lua.LState) int {
	result := L.NewTable()
	url := L.CheckString(1)
	headers := L.CheckTable(2)

	request := httputils.R()
	headers.ForEach(func(k lua.LValue, v lua.LValue) {
		request.AddHeader(LvToString(k), LvToString(v))
	})

	response, err := request.Get(url)
	if err != nil {
		result.RawSet(lua.LString("error"), lua.LString(err.Error()))
		L.Push(result)
		return 1
	}

	result.RawSet(lua.LString("status_code"), lua.LNumber(response.StatusCode()))
	result.RawSet(lua.LString("body"), lua.LString(response.ToString()))
	L.Push(result)
	return 1
}

func doDelete(L *lua.LState) int {
	result := L.NewTable()
	url := L.CheckString(1)
	headers := L.CheckTable(2)

	request := httputils.R()
	headers.ForEach(func(k lua.LValue, v lua.LValue) {
		request.AddHeader(LvToString(k), LvToString(v))
	})

	response, err := request.Delete(url)
	if err != nil {
		result.RawSet(lua.LString("error"), lua.LString(err.Error()))
		L.Push(result)
		return 1
	}

	result.RawSet(lua.LString("status_code"), lua.LNumber(response.StatusCode()))
	result.RawSet(lua.LString("body"), lua.LString(response.ToString()))
	L.Push(result)
	return 1
}

func doPost(L *lua.LState) int {
	result := L.NewTable()
	url := L.CheckString(1)
	headers := L.CheckTable(2)
	body := L.CheckAny(3)

	if !LvIsTable(headers) {
		result.RawSet(lua.LString("error"), lua.LString("The argument 'headers' must Table"))
		L.Push(result)
		return 1
	}

	if !LvIsTable(body) {
		result.RawSet(lua.LString("error"), lua.LString("The argument 'body' must Table"))
		L.Push(result)
		return 1
	}

	data, err := LvToMap(body)
	if err != nil {
		result.RawSet(lua.LString("error"), lua.LString(err.Error()))
		L.Push(result)
		return 1
	}

	request := httputils.R()
	headers.ForEach(func(k lua.LValue, v lua.LValue) {
		request.AddHeader(LvToString(k), LvToString(v))
	})

	var response *httputils.HttpResponse
	response, err = request.SetJson(data).Post(url)
	if err != nil {
		result.RawSet(lua.LString("error"), lua.LString(err.Error()))
		L.Push(result)
		return 1
	}

	result.RawSet(lua.LString("status_code"), lua.LNumber(response.StatusCode()))
	result.RawSet(lua.LString("body"), lua.LString(response.ToString()))
	L.Push(result)
	return 1
}

func doPut(L *lua.LState) int {
	result := L.NewTable()
	url := L.CheckString(1)
	headers := L.CheckTable(2)
	body := L.CheckAny(3)

	if !LvIsTable(headers) {
		result.RawSet(lua.LString("error"), lua.LString("The argument 'headers' must Table"))
		L.Push(result)
		return 1
	}

	if !LvIsTable(body) {
		result.RawSet(lua.LString("error"), lua.LString("The argument 'body' must Table"))
		L.Push(result)
		return 1
	}

	data, err := LvToMap(body)
	if err != nil {
		result.RawSet(lua.LString("error"), lua.LString(err.Error()))
		L.Push(result)
		return 1
	}

	request := httputils.R()
	headers.ForEach(func(k lua.LValue, v lua.LValue) {
		request.AddHeader(LvToString(k), LvToString(v))
	})

	var response *httputils.HttpResponse
	response, err = request.SetJson(data).Put(url)
	if err != nil {
		result.RawSet(lua.LString("error"), lua.LString(err.Error()))
		L.Push(result)
		return 1
	}

	result.RawSet(lua.LString("status_code"), lua.LNumber(response.StatusCode()))
	result.RawSet(lua.LString("body"), lua.LString(response.ToString()))
	L.Push(result)
	return 1
}
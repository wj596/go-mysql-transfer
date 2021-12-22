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
	"github.com/yuin/gopher-lua"

	"go-mysql-transfer/domain/constants"
)

func preloadHttpApiModule(L *lua.LState) {
	L.PreloadModule("httpapi", httpApiModuleLoader)
}

func httpApiModuleLoader(L *lua.LState) int {
	t := L.NewTable()
	L.SetFuncs(t, _httpApi)
	L.Push(t)
	return 1
}

var _httpApi = map[string]lua.LGFunction{
	"POST": httpApiPost,
}

func httpApiPost(L *lua.LState) int {
	headers := L.CheckTable(1)
	value := L.CheckAny(2)

	data := L.NewTable()
	L.SetTable(data, lua.LString("headers"), headers)
	L.SetTable(data, lua.LString("value"), value)

	result := L.GetGlobal(constants.LuaGlobalVariableResult)
	L.SetTable(result, lua.LString(nextId()), data)
	return 0
}

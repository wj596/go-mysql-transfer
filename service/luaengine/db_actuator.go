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
	"github.com/layeh/gopher-json"
	"github.com/yuin/gopher-lua"

	"go-mysql-transfer/util/logs"
)

func dbModule(L *lua.LState) int {
	t := L.NewTable()
	L.SetFuncs(t, _dbModuleApi)
	L.Push(t)
	return 1
}

var _dbModuleApi = map[string]lua.LGFunction{
	"selectOne": selectOne,
	"select":    selectList,
}

func selectOne(L *lua.LState) int {
	sql := L.CheckString(1)

	logs.Infof("lua db module execute sql: %s", sql)

	rs, err := _ds.Execute(sql)
	if err != nil {
		logs.Error(err.Error())
		L.Push(lua.LNil)
		L.Push(lua.LString(err.Error()))
		return 2
	}
	rowNumber := rs.RowNumber()

	table := L.NewTable()
	if rowNumber > 1 {
		logs.Error("return more than 1 row")
		L.Push(lua.LNil)
		L.Push(lua.LString("return more than 1 row"))
		return 2
	}

	if rowNumber == 1 {
		for field, index := range rs.FieldNames {
			v, err := rs.GetValue(0, index)
			if err != nil {
				logs.Error(err.Error())
				L.Push(lua.LNil)
				L.Push(lua.LString(err.Error()))
				return 2
			}
			val := interfaceToLv(v)
			L.SetTable(table, lua.LString(field), val)
		}
	}

	if data, err := json.Encode(table); err == nil {
		logs.Infof("lua db module result: %s", string(data))
	}

	L.Push(table)
	return 1
}

func selectList(L *lua.LState) int {
	sql := L.CheckString(1)

	logs.Infof("lua db module execute sql: %s", sql)

	rs, err := _ds.Execute(sql)
	if err != nil {
		logs.Error(err.Error())
		L.Push(lua.LNil)
		L.Push(lua.LString(err.Error()))
		return 2
	}
	rowNumber := rs.RowNumber()

	ret := L.NewTable()
	if rowNumber > 0 {
		for i := 0; i < rowNumber; i++ {
			table := L.NewTable()
			for field, index := range rs.FieldNames {
				v, err := rs.GetValue(i, index)
				if err != nil {
					logs.Error(err.Error())
					L.Push(lua.LNil)
					L.Push(lua.LString(err.Error()))
					return 2
				}
				val := interfaceToLv(v)
				L.SetTable(table, lua.LString(field), val)
			}
			L.SetTable(ret, lua.LNumber(i+1), table)
		}
	}

	if data, err := json.Encode(ret); err == nil {
		logs.Infof("lua db module result: %s", string(data))
	} else {
		logs.Error(err.Error())
	}

	L.Push(ret)
	return 1
}

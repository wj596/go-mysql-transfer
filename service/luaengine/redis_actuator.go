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
	lua "github.com/yuin/gopher-lua"

	"go-mysql-transfer/global"
	"go-mysql-transfer/util/stringutil"
)

func redisModule(L *lua.LState) int {
	t := L.NewTable()
	L.SetFuncs(t, _redisModuleApi)
	L.Push(t)
	return 1
}

var _redisModuleApi = map[string]lua.LGFunction{
	"rawRow":    rawRow,
	"rawAction": rawAction,

	"SET": redisSet,
	"DEL": redisDel,

	"HSET": redisHSet,
	"HDEL": redisHDel,

	"RPUSH": redisRPush,
	"LREM":  redisLRem,

	"SADD": redisSAdd,
	"SREM": redisSRem,
}

func redisSet(L *lua.LState) int {
	key := L.CheckString(1)
	val := L.CheckAny(2)
	ret := L.GetGlobal(_globalRET)
	L.SetTable(ret, lua.LString("insert_1_"+key), val)
	return 0
}

func redisDel(L *lua.LState) int {
	key := L.CheckString(1)
	ret := L.GetGlobal(_globalRET)
	L.SetTable(ret, lua.LString("delete_1_"+key), lua.LBool(true))
	return 0
}

func redisHSet(L *lua.LState) int {
	key := L.CheckString(1)
	field := L.CheckAny(2)
	val := L.CheckAny(3)

	hash := L.NewTable()
	L.SetTable(hash, lua.LString("key"), lua.LString(key))
	L.SetTable(hash, lua.LString("field"), field)
	L.SetTable(hash, lua.LString("val"), val)

	ret := L.GetGlobal(_globalRET)
	L.SetTable(ret, lua.LString("insert_2_"+stringutil.UUID()), hash)
	return 0
}

func redisHDel(L *lua.LState) int {
	key := L.CheckAny(1)
	field := L.CheckAny(2)

	hash := L.NewTable()
	L.SetTable(hash, lua.LString("key"), key)
	L.SetTable(hash, lua.LString("field"), field)
	L.SetTable(hash, lua.LString("val"), lua.LNumber(1))

	ret := L.GetGlobal(_globalRET)
	L.SetTable(ret, lua.LString("delete_2_"+stringutil.UUID()), hash)
	return 0
}

func redisRPush(L *lua.LState) int {
	key := L.CheckString(1)
	val := L.CheckAny(2)

	ret := L.GetGlobal(_globalRET)
	L.SetTable(ret, lua.LString("insert_3_"+key), val)
	return 0
}

func redisLRem(L *lua.LState) int {
	key := L.CheckString(1)
	val := L.CheckAny(2)

	ret := L.GetGlobal(_globalRET)
	L.SetTable(ret, lua.LString("delete_3_"+key), val)
	return 0
}

func redisSAdd(L *lua.LState) int {
	key := L.CheckString(1)
	val := L.CheckAny(2)

	ret := L.GetGlobal(_globalRET)
	L.SetTable(ret, lua.LString("insert_4_"+key), val)
	return 0
}

func redisSRem(L *lua.LState) int {
	key := L.CheckString(1)
	val := L.CheckAny(2)

	ret := L.GetGlobal(_globalRET)
	L.SetTable(ret, lua.LString("delete_4_"+key), val)
	return 0
}

func DoRedisOps(input map[string]interface{}, action string, rule *global.Rule) ([]*global.RedisRespond, error) {
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

	ls := make([]*global.RedisRespond, 0, ret.Len())
	ret.ForEach(func(k lua.LValue, v lua.LValue) {
		resp := global.RedisRespondPool.Get().(*global.RedisRespond)
		kk := lvToString(k)
		action := kk[0:6]
		structure := kk[7:8]

		resp.Action = action
		resp.Structure = structure
		if structure == global.RedisStructureHash {
			key := L.GetTable(v, lua.LString("key"))
			field := L.GetTable(v, lua.LString("field"))
			val := L.GetTable(v, lua.LString("val"))
			resp.Key = key.String()
			resp.Field = lvToString(field)
			resp.Val = lvToInterface(val, true)
		} else {
			resp.Key = kk[9:len(kk)]
			resp.Val = lvToInterface(v, true)
		}

		ls = append(ls, resp)
	})

	return ls, nil
}

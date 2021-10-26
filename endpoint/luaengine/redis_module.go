package luaengine

import (
	"github.com/yuin/gopher-lua"

	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/util/incrementer"
)

func preloadRedisModule(L *lua.LState) {
	L.PreloadModule("redis", redisModuleLoader)
}

func redisModuleLoader(L *lua.LState) int {
	t := L.NewTable()
	L.SetFuncs(t, redisApis)
	L.Push(t)
	return 1
}

var redisApis = map[string]lua.LGFunction{
	"SET": redisApiSet,
	"DEL": redisApiDel,

	"HSET": redisApiHashSet,
	"HDEL": redisApiHashDel,

	"RPUSH": redisApiRightPush,
	"LREM":  redisApiLeftRem,

	"SADD": redisApiSetAdd,
	"SREM": redisApiSetRem,

	"ZADD": redisApiSortedSetAdd,
	"ZREM": redisApiSortedSetRem,

	"EXPIRE": redisApiExpire,
}

func redisApiSet(L *lua.LState) int {
	key := L.CheckString(1)
	val := L.CheckAny(2)
	result := L.GetGlobal(constants.LuaGlobalVariableResult)
	L.SetTable(result, lua.LString("insert_1_"+key), val)
	return 0
}

func redisApiDel(L *lua.LState) int {
	key := L.CheckString(1)
	result := L.GetGlobal(constants.LuaGlobalVariableResult)
	L.SetTable(result, lua.LString("delete_1_"+key), lua.LBool(true))
	return 0
}

func redisApiHashSet(L *lua.LState) int {
	key := L.CheckString(1)
	field := L.CheckAny(2)
	val := L.CheckAny(3)

	hash := L.NewTable()
	L.SetTable(hash, lua.LString("key"), lua.LString(key))
	L.SetTable(hash, lua.LString("field"), field)
	L.SetTable(hash, lua.LString("val"), val)

	result := L.GetGlobal(constants.LuaGlobalVariableResult)
	L.SetTable(result, lua.LString("insert_2_"+incrementer.NextStr()), hash)
	return 0
}

func redisApiHashDel(L *lua.LState) int {
	key := L.CheckAny(1)
	field := L.CheckAny(2)

	hash := L.NewTable()
	L.SetTable(hash, lua.LString("key"), key)
	L.SetTable(hash, lua.LString("field"), field)
	L.SetTable(hash, lua.LString("val"), lua.LNumber(1))

	result := L.GetGlobal(constants.LuaGlobalVariableResult)
	L.SetTable(result, lua.LString("delete_2_"+incrementer.NextStr()), hash)
	return 0
}

func redisApiRightPush(L *lua.LState) int {
	key := L.CheckString(1)
	val := L.CheckAny(2)

	result := L.GetGlobal(constants.LuaGlobalVariableResult)
	L.SetTable(result, lua.LString("insert_3_"+key), val)
	return 0
}

func redisApiLeftRem(L *lua.LState) int {
	key := L.CheckString(1)
	val := L.CheckAny(2)

	result := L.GetGlobal(constants.LuaGlobalVariableResult)
	L.SetTable(result, lua.LString("delete_3_"+key), val)
	return 0
}

func redisApiSetAdd(L *lua.LState) int {
	key := L.CheckString(1)
	val := L.CheckAny(2)

	result := L.GetGlobal(constants.LuaGlobalVariableResult)
	L.SetTable(result, lua.LString("insert_4_"+key), val)
	return 0
}

func redisApiSetRem(L *lua.LState) int {
	key := L.CheckString(1)
	val := L.CheckAny(2)

	result := L.GetGlobal(constants.LuaGlobalVariableResult)
	L.SetTable(result, lua.LString("delete_4_"+key), val)
	return 0
}

func redisApiSortedSetAdd(L *lua.LState) int {
	key := L.CheckString(1)
	score := L.CheckAny(2)
	val := L.CheckAny(3)

	hash := L.NewTable()
	L.SetTable(hash, lua.LString("key"), lua.LString(key))
	L.SetTable(hash, lua.LString("score"), score)
	L.SetTable(hash, lua.LString("val"), val)

	result := L.GetGlobal(constants.LuaGlobalVariableResult)
	L.SetTable(result, lua.LString("insert_5_"+incrementer.NextStr()), hash)
	return 0
}

func redisApiSortedSetRem(L *lua.LState) int {
	key := L.CheckString(1)
	val := L.CheckAny(2)

	result := L.GetGlobal(constants.LuaGlobalVariableResult)
	L.SetTable(result, lua.LString("delete_5_"+key), val)
	return 0
}

func redisApiExpire(L *lua.LState) int {
	key := L.CheckString(1)
	val := L.CheckNumber(2)

	result := L.GetGlobal(constants.LuaGlobalVariableResult)
	L.SetTable(result, lua.LString("expire_"+key), val)
	return 0
}

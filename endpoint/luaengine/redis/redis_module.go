package redis

import (
	"github.com/yuin/gopher-lua"

	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/util/incrementer"
)

func Preload(L *lua.LState) {
	L.PreloadModule("redis", Loader)
}

func Loader(L *lua.LState) int {
	t := L.NewTable()
	L.SetFuncs(t, api)
	L.Push(t)
	return 1
}

var api = map[string]lua.LGFunction{
	"SET": Set,
	"DEL": Del,

	"HSET": HashSet,
	"HDEL": HashDel,

	"RPUSH": RightPush,
	"LREM":  LeftRem,

	"SADD": SetAdd,
	"SREM": SetRem,

	"ZADD": SortedSetAdd,
	"ZREM": SortedSetRem,

	"EXPIRE": Expire,
}

func Set(L *lua.LState) int {
	key := L.CheckString(1)
	val := L.CheckAny(2)
	result := L.GetGlobal(constants.LuaGlobalVariableResult)
	L.SetTable(result, lua.LString("insert_1_"+key), val)
	return 0
}
func Del(L *lua.LState) int {
	key := L.CheckString(1)
	result := L.GetGlobal(constants.LuaGlobalVariableResult)
	L.SetTable(result, lua.LString("delete_1_"+key), lua.LBool(true))
	return 0
}

func HashSet(L *lua.LState) int {
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
func HashDel(L *lua.LState) int {
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

func RightPush(L *lua.LState) int {
	key := L.CheckString(1)
	val := L.CheckAny(2)

	result := L.GetGlobal(constants.LuaGlobalVariableResult)
	L.SetTable(result, lua.LString("insert_3_"+key), val)
	return 0
}
func LeftRem(L *lua.LState) int {
	key := L.CheckString(1)
	val := L.CheckAny(2)

	result := L.GetGlobal(constants.LuaGlobalVariableResult)
	L.SetTable(result, lua.LString("delete_3_"+key), val)
	return 0
}

func SetAdd(L *lua.LState) int {
	key := L.CheckString(1)
	val := L.CheckAny(2)

	result := L.GetGlobal(constants.LuaGlobalVariableResult)
	L.SetTable(result, lua.LString("insert_4_"+key), val)
	return 0
}
func SetRem(L *lua.LState) int {
	key := L.CheckString(1)
	val := L.CheckAny(2)

	result := L.GetGlobal(constants.LuaGlobalVariableResult)
	L.SetTable(result, lua.LString("delete_4_"+key), val)
	return 0
}

func SortedSetAdd(L *lua.LState) int {
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
func SortedSetRem(L *lua.LState) int {
	key := L.CheckString(1)
	val := L.CheckAny(2)

	result := L.GetGlobal(constants.LuaGlobalVariableResult)
	L.SetTable(result, lua.LString("delete_5_"+key), val)
	return 0
}

func Expire(L *lua.LState) int {
	key := L.CheckString(1)
	val := L.CheckNumber(2)

	result := L.GetGlobal(constants.LuaGlobalVariableResult)
	L.SetTable(result, lua.LString("expire_"+key), val)
	return 0
}

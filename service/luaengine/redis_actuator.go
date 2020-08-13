package luaengine

import (
	lua "github.com/yuin/gopher-lua"

	"go-mysql-transfer/global"
	"go-mysql-transfer/util/stringutil"
)

const (
	_globalNEST = "___NEST___"
)

func redisModule(L *lua.LState) int {
	t := L.NewTable()
	L.SetFuncs(t, _redisModuleApi)
	L.Push(t)
	return 1
}

var _redisModuleApi = map[string]lua.LGFunction{
	"SET":   redisSet,
	"HSET":  redisHSet,
	"RPUSH": redisRPush,
	"SADD":  redisSAdd,
}

func redisSet(L *lua.LState) int {
	key := L.CheckAny(1)
	val := L.CheckAny(2)
	vls := L.GetGlobal(_globalVLS)
	L.SetTable(vls, key, val)
	L.SetGlobal(_globalNEST, lua.LBool(false))
	return 0
}

func redisHSet(L *lua.LState) int {
	key := L.CheckAny(1)
	field := L.CheckAny(2)
	val := L.CheckAny(3)
	hash := L.NewTable()
	L.SetTable(hash, lua.LString("key"), key)
	L.SetTable(hash, lua.LString("field"), field)
	L.SetTable(hash, lua.LString("val"), val)

	vls := L.GetGlobal(_globalVLS)
	L.SetTable(vls, lua.LString(stringutil.UUID()), hash)
	L.SetGlobal(_globalNEST, lua.LBool(true))
	return 0
}

func redisRPush(L *lua.LState) int {
	key := L.CheckAny(1)
	val := L.CheckAny(2)
	vls := L.GetGlobal(_globalVLS)
	L.SetTable(vls, key, val)
	L.SetGlobal(_globalNEST, lua.LBool(false))
	return 0
}

func redisSAdd(L *lua.LState) int {
	key := L.CheckAny(1)
	val := L.CheckAny(2)
	vls := L.GetGlobal(_globalVLS)
	L.SetTable(vls, key, val)
	L.SetGlobal(_globalNEST, lua.LBool(false))
	return 0
}

func DoRedisOps(input map[string]interface{}, rule *global.Rule) ([]*global.RedisRespond, error) {
	L := _pool.Get()
	defer _pool.Put(L)

	row := L.NewTable()
	paddingTable(L, row, input)
	vls := L.NewTable()
	L.SetGlobal(_globalVLS, vls)

	funcFromProto := L.NewFunctionFromProto(rule.LuaProto)
	L.Push(funcFromProto)
	err := L.PCall(0, lua.MultRet, nil)
	if err != nil {
		return nil, err
	}

	fn := L.GetGlobal(_globalTransferFunc)
	err = L.CallByParam(lua.P{
		Fn:      fn,
		NRet:    0,
		Protect: true,
	}, row)

	if err != nil {
		return nil, err
	}

	nest := lua.LVAsBool(L.GetGlobal(_globalNEST))
	ls := make([]*global.RedisRespond, 0, vls.Len())
	vls.ForEach(func(k lua.LValue, v lua.LValue) {
		resp := global.RedisRespondPool.Get().(*global.RedisRespond)
		if nest {
			key := L.GetTable(v, lua.LString("key"))
			field := L.GetTable(v, lua.LString("field"))
			val := L.GetTable(v, lua.LString("val"))
			resp.Key = decodeString(key)
			resp.Field = decodeString(field)
			resp.Val = decodeValue(val)
		} else {
			resp.Key = decodeString(k)
			resp.Field = ""
			resp.Val = decodeValue(v)
		}

		ls = append(ls, resp)
	})

	return ls, nil
}

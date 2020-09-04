package luaengine

import (
	lua "github.com/yuin/gopher-lua"

	"go-mysql-transfer/global"
)

func mqModule(L *lua.LState) int {
	t := L.NewTable()
	L.SetFuncs(t, _mqModuleApi)
	L.Push(t)
	return 1
}

var _mqModuleApi = map[string]lua.LGFunction{
	"rawRow":    rawRow,
	"rawAction": rawAction,

	"SEND": msgSend,
}

func msgSend(L *lua.LState) int {
	topic := L.CheckAny(1)
	msg := L.CheckAny(2)

	ret := L.GetGlobal(_globalRET)
	L.SetTable(ret, msg, topic)
	return 0
}

func DoMQOps(input map[string]interface{}, action string, rule *global.Rule) ([]*global.MQRespond, error) {
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

	list := make([]*global.MQRespond, 0, ret.Len())
	ret.ForEach(func(k lua.LValue, v lua.LValue) {
		resp := global.MQRespondPool.Get().(*global.MQRespond)
		resp.ByteArray = lvToByteArray(k)
		resp.Topic = lvToString(v)
		list = append(list, resp)
	})

	return list, nil
}

package luaengine

import (
	lua "github.com/yuin/gopher-lua"
	"go-mysql-transfer/global"
)

func rocketmqModule(L *lua.LState) int {
	t := L.NewTable()
	L.SetFuncs(t, _rocketmqModuleApi)
	L.Push(t)
	return 1
}

var _rocketmqModuleApi = map[string]lua.LGFunction{
	"SEND": rocketmqSend,
}

func rocketmqSend(L *lua.LState) int {
	topic := L.CheckAny(1)
	msg := L.CheckAny(2)
	vls := L.GetGlobal(_globalVLS)
	L.SetTable(vls, msg, topic)
	return 0
}

func DoRocketmqOps(input map[string]interface{}, rule *global.Rule) ([]*global.RocketmqRespond, error) {
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

	list := make([]*global.RocketmqRespond, 0, vls.Len())
	vls.ForEach(func(k lua.LValue, v lua.LValue) {
		resp := global.RocketmqRespondPool.Get().(*global.RocketmqRespond)
		resp.Msg = decodeByteArray(k)
		resp.Topic = decodeString(k)
		list = append(list, resp)
	})

	return list, nil
}

package luaengine

import (
	lua "github.com/yuin/gopher-lua"

	"go-mysql-transfer/global"
)

func scriptModule(L *lua.LState) int {
	t := L.NewTable()
	L.SetFuncs(t, _scriptModuleApi)
	L.Push(t)
	return 1
}

var _scriptModuleApi = map[string]lua.LGFunction{
	"rawRow":    rawRow,
	"rawAction": rawAction,
}

func DoScript(input map[string]interface{}, action string, rule *global.Rule) error {
	L := _pool.Get()
	defer _pool.Put(L)

	row := L.NewTable()
	paddingTable(L, row, input)

	L.SetGlobal(_globalROW, row)
	L.SetGlobal(_globalACT, lua.LString(action))

	funcFromProto := L.NewFunctionFromProto(rule.LuaProto)
	L.Push(funcFromProto)
	err := L.PCall(0, lua.MultRet, nil)
	if err != nil {
		return err
	}

	return nil
}

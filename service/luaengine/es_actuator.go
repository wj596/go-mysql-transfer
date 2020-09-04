package luaengine

import (
	"github.com/siddontang/go-mysql/canal"
	lua "github.com/yuin/gopher-lua"

	"go-mysql-transfer/global"
	"go-mysql-transfer/util/stringutil"
)

func esModule(L *lua.LState) int {
	t := L.NewTable()
	L.SetFuncs(t, _esModuleApi)
	L.Push(t)
	return 1
}

var _esModuleApi = map[string]lua.LGFunction{
	"rawRow":    rawRow,
	"rawAction": rawAction,

	"INSERT": esInsert,
	"UPDATE": esUpdate,
	"DELETE": esDelete,
}

func esInsert(L *lua.LState) int {
	index := L.CheckAny(1)
	id := L.CheckAny(2)
	body := L.CheckAny(3)

	data := L.NewTable()
	L.SetTable(data, lua.LString("index"), index)
	L.SetTable(data, lua.LString("action"), lua.LString(canal.InsertAction))
	L.SetTable(data, lua.LString("id"), id)
	L.SetTable(data, lua.LString("body"), body)

	ret := L.GetGlobal(_globalRET)
	L.SetTable(ret, lua.LString(stringutil.UUID()), data)
	return 0
}

func esUpdate(L *lua.LState) int {
	index := L.CheckAny(1)
	id := L.CheckAny(2)
	body := L.CheckAny(3)

	data := L.NewTable()
	L.SetTable(data, lua.LString("index"), index)
	L.SetTable(data, lua.LString("action"), lua.LString(canal.UpdateAction))
	L.SetTable(data, lua.LString("id"), id)
	L.SetTable(data, lua.LString("body"), body)

	ret := L.GetGlobal(_globalRET)
	L.SetTable(ret, lua.LString(stringutil.UUID()), data)
	return 0
}

func esDelete(L *lua.LState) int {
	index := L.CheckAny(1)
	id := L.CheckAny(2)

	data := L.NewTable()
	L.SetTable(data, lua.LString("index"), index)
	L.SetTable(data, lua.LString("action"), lua.LString(canal.DeleteAction))
	L.SetTable(data, lua.LString("id"), id)

	ret := L.GetGlobal(_globalRET)
	L.SetTable(ret, lua.LString(stringutil.UUID()), data)
	return 0
}

func DoESOps(input map[string]interface{}, action string, rule *global.Rule) ([]*global.ESRespond, error) {
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

	responds := make([]*global.ESRespond, 0, ret.Len())
	ret.ForEach(func(k lua.LValue, v lua.LValue) {
		resp := new(global.ESRespond)
		resp.Index = lvToString(L.GetTable(v, lua.LString("index")))
		resp.Id = lvToString(L.GetTable(v, lua.LString("id")))
		resp.Action = lvToString(L.GetTable(v, lua.LString("action")))

		var data string
		body := L.GetTable(v, lua.LString("body"))
		switch body.Type() {
		case lua.LTNumber:
			data = lua.LVAsString(body)
		case lua.LTString:
			data = lua.LVAsString(body)
		case lua.LTTable:
			mm, _ := lvToMap(body)
			data = stringutil.ToJsonString(mm)
		default:
			data = stringutil.ToJsonString(body)
		}
		resp.Date = data
		responds = append(responds, resp)
	})

	return responds, nil
}

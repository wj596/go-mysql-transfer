package db

import (
	"github.com/yuin/gopher-lua"

	"go-mysql-transfer/domain/constants"
)

func Preload(L *lua.LState) {
	L.PreloadModule("db", Loader)
}

func Loader(L *lua.LState) int {
	t := L.NewTable()
	L.SetFuncs(t, api)
	L.Push(t)
	return 1
}

var api = map[string]lua.LGFunction{
	"getRow":    GetRow,
	"getPreRow": GetPreRow,
	"getAction": GetAction,
}

func GetRow(L *lua.LState) int {
	row := L.GetGlobal(constants.LuaGlobalVariableRow)
	L.Push(row)
	return 1
}

func GetPreRow(L *lua.LState) int {
	row := L.GetGlobal(constants.LuaGlobalVariablePreRow)
	L.Push(row)
	return 1
}

func GetAction(L *lua.LState) int {
	act := L.GetGlobal(constants.LuaGlobalVariableAction)
	L.Push(act)
	return 1
}

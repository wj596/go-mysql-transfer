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
	"getRow":        GetCurrentRow,
	"getCoveredRow": GetCoveredRow,
	"getAction":     GetAction,
}

func GetCurrentRow(L *lua.LState) int {
	row := L.GetGlobal(constants.LuaGlobalVariableCurrentRow)
	L.Push(row)
	return 1
}

func GetCoveredRow(L *lua.LState) int {
	row := L.GetGlobal(constants.LuaGlobalVariableCoveredRow)
	L.Push(row)
	return 1
}

func GetAction(L *lua.LState) int {
	act := L.GetGlobal(constants.LuaGlobalVariableAction)
	L.Push(act)
	return 1
}

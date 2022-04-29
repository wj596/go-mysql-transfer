package luaengine

import (
	"strings"
	"testing"

	"github.com/yuin/gopher-lua"
	"github.com/yuin/gopher-lua/parse"

	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/util/fileutils"
	"go-mysql-transfer/util/stringutils"
)

func TestHttpClientModule(t *testing.T) {
	L := New(constants.EndpointTypeHttp)
	luaScript, err := fileutils.ReadAsString("D://test.lua")
	if err != nil {
		t.Fatal(err)
	}

	protoName := stringutils.UUID()
	reader := strings.NewReader(luaScript)
	chunk, err := parse.Parse(reader, protoName)
	if err != nil {
		t.Fatal(err)
	}
	luaFunctionProto, err := lua.Compile(chunk, protoName) //编译
	if err != nil {
		t.Fatal(err)
	}

	L.SetGlobal(GlobalDataSourceName, lua.LString("root:root@tcp(192.168.44.113:3306)/%s?timeout=5s"))

	funcFromProto := L.NewFunctionFromProto(luaFunctionProto)
	L.Push(funcFromProto)
	err = L.PCall(0, lua.MultRet, nil)
	if err != nil {
		L.Close()
		t.Fatal(err)
	}

	event := L.NewTable()

	err = L.CallByParam(lua.P{
		Fn:      L.GetGlobal(HandleFunctionName),
		NRet:    0,
		Protect: true,
	}, event)
	if err != nil {
		t.Fatal(err)
	}
}

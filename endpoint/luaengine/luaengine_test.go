package luaengine

import (
	"fmt"
	"github.com/yuin/gopher-lua"
	"github.com/yuin/gopher-lua/parse"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/util/stringutils"
	"io/ioutil"
	"strings"
	"testing"
)

func TestLuaKey(t *testing.T) {
	kk := "expire_123"
	fmt.Println(kk[0:6])
	fmt.Println(kk[7:len(kk)])
}

func TestLuaCompile(t *testing.T) {
	script, err := ioutil.ReadFile("d://test.lua")
	if err != nil {
		t.Fatal(err)
	}

	protoName := stringutils.UUID()
	reader := strings.NewReader(string(script))
	chunk, err := parse.Parse(reader, protoName)
	if err != nil {
		t.Fatal(err)
	}
	proto, err := lua.Compile(chunk, protoName)
	if err != nil {
		t.Fatal(err)
	}
	proto = nil
	fmt.Println(proto)
}

func TestExecuteRedisModule(t *testing.T) {
	L := _pool.Borrow()
	defer _pool.Release(L)

	script, err := ioutil.ReadFile("d://test.lua")
	if err != nil {
		t.Fatal(err)
	}

	currentRow := make(map[string]interface{})
	currentRow["name"] = "zhangsan"
	currentRow["age"] = 22
	current := L.NewTable()
	padLuaTable(L, current, currentRow)
	L.SetGlobal(constants.LuaGlobalVariableCurrentRow, current)

	result := L.NewTable()
	L.SetGlobal(constants.LuaGlobalVariableResult, result)

	protoName := stringutils.UUID()
	reader := strings.NewReader(string(script))
	chunk, _ := parse.Parse(reader, protoName)
	proto, _ := lua.Compile(chunk, protoName)
	funcFromProto := L.NewFunctionFromProto(proto)
	L.Push(funcFromProto)
	err = L.PCall(0, lua.MultRet, nil)
	if err != nil {
		t.Fatal(err)
	}

	result.ForEach(func(k lua.LValue, v lua.LValue) {
		fmt.Println(k)
		fmt.Println(luaValueToInterface(v, true))
	})

}

package luaengine

import (
	"encoding/json"
	lua "github.com/yuin/gopher-lua"

	"go-mysql-transfer/util/byteutil"
	"go-mysql-transfer/util/stringutil"
)

const (
	_globalRET = "___RET___"
	_globalROW = "___ROW___"
	_globalACT = "___ACT___"
)

func rawRow(L *lua.LState) int {
	row := L.GetGlobal(_globalROW)
	L.Push(row)
	return 1
}

func rawAction(L *lua.LState) int {
	act := L.GetGlobal(_globalACT)
	L.Push(act)
	return 1
}

func paddingTable(l *lua.LState, table *lua.LTable, kv map[string]interface{}) {
	for k, v := range kv {
		switch v.(type) {
		case float64:
			ft := v.(float64)
			l.SetTable(table, lua.LString(k), lua.LNumber(ft))
		case float32:
			ft := v.(float32)
			l.SetTable(table, lua.LString(k), lua.LNumber(ft))
		case int:
			ft := v.(int)
			l.SetTable(table, lua.LString(k), lua.LNumber(ft))
		case uint:
			ft := v.(uint)
			l.SetTable(table, lua.LString(k), lua.LNumber(ft))
		case int8:
			ft := v.(int8)
			l.SetTable(table, lua.LString(k), lua.LNumber(ft))
		case uint8:
			ft := v.(uint8)
			l.SetTable(table, lua.LString(k), lua.LNumber(ft))
		case int16:
			ft := v.(int16)
			l.SetTable(table, lua.LString(k), lua.LNumber(ft))
		case uint16:
			ft := v.(uint16)
			l.SetTable(table, lua.LString(k), lua.LNumber(ft))
		case int32:
			ft := v.(int32)
			l.SetTable(table, lua.LString(k), lua.LNumber(ft))
		case uint32:
			ft := v.(uint32)
			l.SetTable(table, lua.LString(k), lua.LNumber(ft))
		case int64:
			ft := v.(int64)
			l.SetTable(table, lua.LString(k), lua.LNumber(ft))
		case uint64:
			ft := v.(uint64)
			l.SetTable(table, lua.LString(k), lua.LNumber(ft))
		case string:
			ft := v.(string)
			l.SetTable(table, lua.LString(k), lua.LString(ft))
		case []byte:
			ft := string(v.([]byte))
			l.SetTable(table, lua.LString(k), lua.LString(ft))
		case nil:
			l.SetTable(table, lua.LString(k), lua.LNil)
		default:
			jsonValue, _ := json.Marshal(v)
			l.SetTable(table, lua.LString(k), lua.LString(jsonValue))
		}
	}
}

func lvToString(lv lua.LValue) string {
	if lua.LVCanConvToString(lv) {
		return lua.LVAsString(lv)
	}

	return lv.String()
}

func lvToByteArray(lv lua.LValue) []byte {
	switch lv.Type() {
	case lua.LTNil:
		return nil
	case lua.LTBool:
		return byteutil.JsonBytes(lua.LVAsBool(lv))
	case lua.LTNumber:
		return []byte(lv.String())
	case lua.LTString:
		return []byte(lua.LVAsString(lv))
	case lua.LTTable:
		ret := lvToInterface(lv, false)
		return byteutil.JsonBytes(ret)
	default:
		return byteutil.JsonBytes(lv)
	}
}

func lvToInterface(lv lua.LValue, tableToJson bool) interface{} {
	switch lv.Type() {
	case lua.LTNil:
		return nil
	case lua.LTBool:
		return lua.LVAsBool(lv)
	case lua.LTNumber:
		return float64(lua.LVAsNumber(lv))
	case lua.LTString:
		return lua.LVAsString(lv)
	case lua.LTTable:
		t, _ := lv.(*lua.LTable)
		len := t.MaxN()
		if len == 0 { // table
			ret := make(map[string]interface{})
			t.ForEach(func(key, value lua.LValue) {
				ret[lvToString(key)] = lvToInterface(value, false)
			})
			if tableToJson {
				return stringutil.ToJsonString(ret)
			}
			return ret
		} else { // array
			ret := make([]interface{}, 0, len)
			for i := 1; i <= len; i++ {
				ret = append(ret, lvToInterface(t.RawGetInt(i), false))
			}
			if tableToJson {
				return stringutil.ToJsonString(ret)
			}
			return ret
		}
	default:
		return lv
	}
}

func lvToMap(lv lua.LValue) (map[string]interface{}, bool) {
	switch lv.Type() {
	case lua.LTTable:
		t := lvToInterface(lv, false)
		ret := t.(map[string]interface{})
		return ret, true
	default:
		return nil, false
	}
}

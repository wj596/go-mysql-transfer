package luaengine

import (
	"encoding/json"
	"go-mysql-transfer/util/byteutil"
	"go-mysql-transfer/util/stringutil"

	lua "github.com/yuin/gopher-lua"
)

const (
	_globalVLS          = "___VLS___"
	_globalTransferFunc = "transfer"
)

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

func decodeString(lv lua.LValue) string {
	if lua.LVCanConvToString(lv) {
		return lua.LVAsString(lv)
	}

	return lv.String()
}

func decodeValue(lv lua.LValue) interface{} {
	switch lv.Type() {
	case lua.LTTable:
		t, ok := lv.(*lua.LTable)
		if ok {
			ret := make(map[string]interface{})
			t.ForEach(func(k lua.LValue, v lua.LValue) {
				key := stringutil.ToString(decodeBasicValue(k))
				ret[key] = decodeBasicValue(v)
			})
			return ret
		}
		return nil
	default:
		return decodeBasicValue(lv)
	}
}

func decodeBasicValue(lv lua.LValue) interface{} {
	switch lv.Type() {
	case lua.LTNil:
		return nil
	case lua.LTBool:
		return lua.LVAsBool(lv)
	case lua.LTNumber:
		return float64(lua.LVAsNumber(lv))
	case lua.LTString:
		return lua.LVAsString(lv)
	default:
		return stringutil.ToJsonString(lv)
	}
}

func decodeByteArray(lv lua.LValue) []byte {
	switch lv.Type() {
	case lua.LTNil:
		return nil
	case lua.LTBool:
		var err error
		var ret []byte
		if lua.LVAsBool(lv) {
			ret, err = byteutil.Uint8ToBytes(1)
		} else {
			ret, err = byteutil.Uint8ToBytes(0)
		}
		if err != nil {
			return nil
		}
		return ret
	case lua.LTNumber:
		t := float64(lua.LVAsNumber(lv))
		return byteutil.Float64ToByte(t)
	case lua.LTString:
		return []byte(lua.LVAsString(lv))
	case lua.LTTable:
		t, ok := lv.(*lua.LTable)
		if ok {
			ret := make(map[string]interface{})
			t.ForEach(func(k lua.LValue, v lua.LValue) {
				key := stringutil.ToString(decodeValue(k))
				ret[key] = decodeValue(v)
			})
			data, err := json.Marshal(ret)
			if err != nil {
				return nil
			}
			return data
		}
		return nil
	default:
		data, err := json.Marshal(lv)
		if err != nil {
			return nil
		}
		return data
	}
}

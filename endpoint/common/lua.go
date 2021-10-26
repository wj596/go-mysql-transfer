package common

import (
	"github.com/json-iterator/go"
	"github.com/yuin/gopher-lua"

	"go-mysql-transfer/util/log"
)

const (
	HandleFunctionName   = "handle"
	RowKey               = lua.LString("Row")
	PreRowKey            = lua.LString("PreRow")
	ActionKey            = lua.LString("Action")
	GlobalVariableResult = "___RESULT___"
)

func PaddingLTable(l *lua.LState, table *lua.LTable, kv map[string]interface{}) {
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
			jsonValue, _ := jsoniter.Marshal(v)
			l.SetTable(table, lua.LString(k), lua.LString(jsonValue))
		}
	}
}

func LValueToString(lv lua.LValue) string {
	if lua.LVCanConvToString(lv) {
		return lua.LVAsString(lv)
	}
	return lv.String()
}

func LValueToInterface(lv lua.LValue, mapToJson bool) interface{} {
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
				ret[LValueToString(key)] = LValueToInterface(value, mapToJson)
			})
			if mapToJson {
				data, err := jsoniter.Marshal(ret)
				if nil != err {
					log.Error(err.Error())
					return nil
				}
				return string(data)
			}
			return ret
		} else { // array
			ret := make([]interface{}, 0, len)
			for i := 1; i <= len; i++ {
				ret = append(ret, LValueToInterface(t.RawGetInt(i), mapToJson))
			}
			if mapToJson {
				data, err := jsoniter.Marshal(ret)
				if nil != err {
					log.Error(err.Error())
					return nil
				}
				return string(data)
			}
			return ret
		}
	default:
		return lv
	}
}

package luaengine

import (
	"strconv"

	"github.com/json-iterator/go"
	"github.com/siddontang/go-mysql/canal"
	"github.com/yuin/gopher-lua"

	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/stringutils"
)

func ExecuteRedisModule(currentRow map[string]interface{}, coveredRow map[string]interface{}, action string, rule *bo.RuntimeRule) ([]*bo.RedisLuaExeResult, error) {
	L := _pool.Borrow()
	defer _pool.Release(L)

	L.SetGlobal(constants.LuaGlobalVariableAction, lua.LString(action))

	current := L.NewTable()
	padLuaTable(L, current, currentRow)
	L.SetGlobal(constants.LuaGlobalVariableCurrentRow, current)

	if action == canal.UpdateAction {
		covered := L.NewTable()
		padLuaTable(L, covered, coveredRow)
		L.SetGlobal(constants.LuaGlobalVariableCoveredRow, covered)
	}

	result := L.NewTable()
	L.SetGlobal(constants.LuaGlobalVariableResult, result)

	funcFromProto := L.NewFunctionFromProto(rule.GetLuaFunctionProto())
	L.Push(funcFromProto)
	err := L.PCall(0, lua.MultRet, nil)
	if err != nil {
		return nil, err
	}

	ls := make([]*bo.RedisLuaExeResult, 0, result.Len())
	result.ForEach(func(k lua.LValue, v lua.LValue) {
		resp := bo.BorrowRedisLuaExeResult()
		kk := luaValueToString(k)
		resp.Action = kk[0:6]
		if constants.ActionExpire == resp.Action {
			resp.Key = kk[7:len(kk)]
			resp.Expiration = stringutils.ToInt64Safe(luaValueToString(v))
		} else {
			resp.Structure, _ = strconv.Atoi(kk[7:8])
			switch resp.Structure {
			case constants.RedisStructureString:
				resp.Key = kk[9:len(kk)]
				resp.Value = luaValueToInterface(v, true)
			case constants.RedisStructureHash:
				if resp.Action == canal.DeleteAction {
					key := L.GetTable(v, lua.LString("key"))
					field := L.GetTable(v, lua.LString("field"))
					resp.Key = key.String()
					resp.Field = luaValueToString(field)
				} else {
					key := L.GetTable(v, lua.LString("key"))
					field := L.GetTable(v, lua.LString("field"))
					val := L.GetTable(v, lua.LString("val"))
					resp.Key = key.String()
					resp.Field = luaValueToString(field)
					resp.Value = luaValueToInterface(val, true)
				}
			case constants.RedisStructureSortedSet:
				if resp.Action == canal.DeleteAction {
					key := L.GetTable(v, lua.LString("key"))
					val := L.GetTable(v, lua.LString("val"))
					resp.Key = key.String()
					resp.Value = luaValueToInterface(val, true)
				} else {
					key := L.GetTable(v, lua.LString("key"))
					score := L.GetTable(v, lua.LString("score"))
					val := L.GetTable(v, lua.LString("val"))
					resp.Key = key.String()
					scoreTemp := luaValueToString(score)
					resp.Score = stringutils.ToFloat64Safe(scoreTemp)
					resp.Value = luaValueToInterface(val, true)
				}
			default:
				resp.Key = kk[9:len(kk)]
				resp.Value = luaValueToInterface(v, true)
			}
			//------end switch
		}
		ls = append(ls, resp)
	})

	return ls, nil
}

func padLuaTable(l *lua.LState, table *lua.LTable, kv map[string]interface{}) {
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

func luaValueToString(lv lua.LValue) string {
	if lua.LVCanConvToString(lv) {
		return lua.LVAsString(lv)
	}
	return lv.String()
}

func luaValueToInterface(lv lua.LValue, mapToJson bool) interface{} {
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
				ret[luaValueToString(key)] = luaValueToInterface(value, mapToJson)
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
				ret = append(ret, luaValueToInterface(t.RawGetInt(i), mapToJson))
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

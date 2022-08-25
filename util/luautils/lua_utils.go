/*
 * Copyright 2021-2022 the original author(https://github.com/wj596)
 *
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package luautils

import (
	"github.com/juju/errors"
	"github.com/yuin/gopher-lua"

	"go-mysql-transfer/util/byteutils"
	"go-mysql-transfer/util/jsonutils"
	"go-mysql-transfer/util/log"
)

func PaddingLuaTableWithMap(l *lua.LState, table *lua.LTable, kv map[string]interface{}) {
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
			data, err := jsonutils.ToJsonByJsoniter(v)
			if nil != err {
				log.Error(err.Error())
			}
			l.SetTable(table, lua.LString(k), lua.LString(data))
		}
	}
}

func PaddingLuaTableWithValue(l *lua.LState, table *lua.LTable, k string, v interface{}) {
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
		data, err := jsonutils.ToJsonByJsoniter(v)
		if nil != err {
			log.Error(err.Error())
		}
		l.SetTable(table, lua.LString(k), lua.LString(data))
	}
}

func toString(lv lua.LValue) string {
	if lua.LVCanConvToString(lv) {
		return lua.LVAsString(lv)
	}
	return lv.String()
}

func LvToInterface(lv lua.LValue, mapToJson bool) interface{} {
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
				ret[toString(key)] = LvToInterface(value, mapToJson)
			})
			if mapToJson {
				data, err := jsonutils.ToJsonByJsoniter(ret)
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
				ret = append(ret, LvToInterface(t.RawGetInt(i), mapToJson))
			}
			if mapToJson {
				data, err := jsonutils.ToJsonByJsoniter(ret)
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

func LvToMap(lv lua.LValue) (map[string]interface{}, error) {
	switch lv.Type() {
	case lua.LTTable:
		t := LvToInterface(lv, false)
		ret := t.(map[string]interface{})
		return ret, nil
	default:
		return nil, errors.New("转换目标必须是LTTable类型")
	}
}

func LvIsTable(lv lua.LValue) bool {
	if nil == lv {
		return false
	}

	switch lv.Type() {
	case lua.LTTable:
		return true
	default:
		return false
	}
}

func LvToByteArray(lv lua.LValue) []byte {
	switch lv.Type() {
	case lua.LTNil:
		return nil
	case lua.LTBool:
		if lua.LVAsBool(lv) {
			return byteutils.StringToBytes("true")
		}
		return byteutils.StringToBytes("false")
	case lua.LTNumber:
		return byteutils.StringToBytes(lua.LVAsString(lv))
	case lua.LTString:
		return byteutils.StringToBytes(lua.LVAsString(lv))
	case lua.LTTable:
		ret := LvToInterface(lv, false)
		return byteutils.ToJsonBytes(ret)
	default:
		return byteutils.ToJsonBytes(lv)
	}
}

func LvToString(lv lua.LValue) string {
	switch lv.Type() {
	case lua.LTNil:
		return ""
	case lua.LTBool:
		if lua.LVAsBool(lv) {
			return "true"
		}
		return "false"
	case lua.LTNumber:
		return lua.LVAsString(lv)
	case lua.LTString:
		return lua.LVAsString(lv)
	case lua.LTTable:
		ret := LvToInterface(lv, false)
		data, err := jsonutils.ToJsonStringByJsoniter(ret)
		if nil != err {
			log.Error(err.Error())
		}
		return data
	default:
		return lv.String()
	}
}

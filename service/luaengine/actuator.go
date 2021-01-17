/*
 * Copyright 2020-2021 the original author(https://github.com/wj596)
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
package luaengine

import (
	"encoding/json"
	"sync"

	luaJson "github.com/layeh/gopher-json"
	"github.com/siddontang/go-mysql/canal"
	"github.com/yuin/gopher-lua"

	"go-mysql-transfer/util/byteutil"
	"go-mysql-transfer/util/httpclient"
	"go-mysql-transfer/util/stringutil"
)

const (
	_globalRET = "___RET___"
	_globalROW = "___ROW___"
	_globalACT = "___ACT___"
)

var (
	_pool *luaStatePool
	_ds   *canal.Canal

	_httpClient *httpclient.HttpClient
)

type luaStatePool struct {
	lock  sync.Mutex
	saved []*lua.LState
}

func InitActuator(ds *canal.Canal) {
	_ds = ds
	_pool = &luaStatePool{
		saved: make([]*lua.LState, 0, 3),
	}
}

func (p *luaStatePool) Get() *lua.LState {
	p.lock.Lock()
	defer p.lock.Unlock()

	n := len(p.saved)
	if n == 0 {
		return p.New()
	}
	x := p.saved[n-1]
	p.saved = p.saved[0 : n-1]
	return x
}

func (p *luaStatePool) New() *lua.LState {
	_httpClient = httpclient.NewClient()

	L := lua.NewState()

	luaJson.Preload(L)

	L.PreloadModule("scriptOps", scriptModule)
	L.PreloadModule("dbOps", dbModule)
	L.PreloadModule("httpOps", httpModule)

	L.PreloadModule("redisOps", redisModule)
	L.PreloadModule("mqOps", mqModule)
	L.PreloadModule("mongodbOps", mongoModule)
	L.PreloadModule("esOps", esModule)

	return L
}

func (p *luaStatePool) Put(L *lua.LState) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.saved = append(p.saved, L)
}

func (p *luaStatePool) Shutdown() {
	for _, L := range p.saved {
		L.Close()
	}
}

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

func interfaceToLv(v interface{}) lua.LValue {
	switch v.(type) {
	case float64:
		ft := v.(float64)
		return lua.LNumber(ft)
	case float32:
		ft := v.(float32)
		return lua.LNumber(ft)
	case int:
		ft := v.(int)
		return lua.LNumber(ft)
	case uint:
		ft := v.(uint)
		return lua.LNumber(ft)
	case int8:
		ft := v.(int8)
		return lua.LNumber(ft)
	case uint8:
		ft := v.(uint8)
		return lua.LNumber(ft)
	case int16:
		ft := v.(int16)
		return lua.LNumber(ft)
	case uint16:
		ft := v.(uint16)
		return lua.LNumber(ft)
	case int32:
		ft := v.(int32)
		return lua.LNumber(ft)
	case uint32:
		ft := v.(uint32)
		return lua.LNumber(ft)
	case int64:
		ft := v.(int64)
		return lua.LNumber(ft)
	case uint64:
		ft := v.(uint64)
		return lua.LNumber(ft)
	case string:
		ft := v.(string)
		return lua.LString(ft)
	case []byte:
		ft := string(v.([]byte))
		return lua.LString(ft)
	case nil:
		return lua.LNil
	default:
		jsonValue, _ := json.Marshal(v)
		return lua.LString(jsonValue)
	}

}

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

package module

import (
	"encoding/json"
	"strconv"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/yuin/gopher-lua"

	"go-mysql-transfer/dao"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/endpoint/redis"
	"go-mysql-transfer/util/byteutils"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/luautils"
	"go-mysql-transfer/util/stringutils"
)

var (
	_compositeClients       = make(map[string]*redis.CompositeClient)
	_lockOfCompositeClients sync.RWMutex
)

func PreloadRedisClientModule(L *lua.LState) {
	L.PreloadModule("redis_client", redisClientLoader)
}

func redisClientLoader(L *lua.LState) int {
	t := L.NewTable()
	L.SetFuncs(t, redisClientApis)
	L.Push(t)
	return 1
}

var redisClientApis = map[string]lua.LGFunction{
	"SET": redisClientSet,
	"DEL": redisClientDel,

	"HSET": redisClientHSet,
	"HDEL": redisClientHDel,

	"RPUSH": redisClientRPush,
	"LREM":  redisClientLRem,

	"SADD": redisClientSAdd,
	"SREM": redisClientSRem,

	"ZADD": redisClientZAdd,
	"ZREM": redisClientZRem,

	"EXPIRE": redisClientExpire,

	"GET":    redisClientGet,
	"INCR":   redisClientIncr,
	"DECR":   redisClientDecr,
	"APPEND": redisClientAppend,

	"HGET":    redisClientHGet,
	"HEXISTS": redisClientHExists,
}

func redisClientSet(L *lua.LState) int {
	key := L.CheckString(1)
	value := L.CheckAny(2)
	if "" == key || nil == value {
		L.Push(lua.LFalse)
		L.Push(lua.LString("参数key和value均不能为空"))
		L.Push(lua.LNil)
		return 3
	}

	cc, err := getCompositeClient(L)
	if err != nil {
		log.Warnf("获取Redis CompositeClient错误[%s]", err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(lua.LNil)
		return 3
	}

	var result string
	result, err = cc.Set(key, value)
	if err != nil {
		log.Warnf("Redis Set: Key[%s]、异常[%s]", key, err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(lua.LNil)
		return 3
	}

	log.Debugf("Redis Set: Key[%s]、结果[%s]", key, result)

	L.Push(lua.LTrue)
	L.Push(lua.LNil)
	L.Push(lua.LString(result))
	return 3
}

func redisClientDel(L *lua.LState) int {
	key := L.CheckString(1)
	if "" == key {
		L.Push(lua.LFalse)
		L.Push(lua.LString("参数key不能为空"))
		L.Push(lua.LNil)
		return 3
	}

	cc, err := getCompositeClient(L)
	if err != nil {
		log.Warnf("获取Redis CompositeClient错误[%s]", err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(lua.LNil)
		return 3
	}

	var result int64
	result, err = cc.Del(key)
	if err != nil {
		log.Warnf("Redis Del: Key[%s]、异常[%s]", key, err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(lua.LNil)
		return 3
	}

	log.Debugf("Redis Del: Key[%s]、结果[%d]", key, result)

	L.Push(lua.LTrue)
	L.Push(lua.LNil)
	L.Push(lua.LNumber(result))
	return 3
}

func redisClientHSet(L *lua.LState) int {
	key := L.CheckString(1)
	field := L.CheckString(2)
	value := L.CheckAny(3)
	if "" == key || "" == field || nil == value {
		L.Push(lua.LFalse)
		L.Push(lua.LString("参数key、field、value均不能为空"))
		L.Push(lua.LNil)
		return 3
	}

	cc, err := getCompositeClient(L)
	if err != nil {
		log.Warnf("获取Redis CompositeClient错误[%s]", err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(lua.LNil)
		return 3
	}

	var result bool
	result, err = cc.HSet(key, field, luautils.LvToInterface(value, true))
	if err != nil {
		log.Warnf("Redis HSet: Key[%s]、Field[%s]、异常[%s]", key, field, err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(lua.LNil)
		return 3
	}

	log.Debugf("Redis HSet: Key[%s]、Field[%v]、结果[%v]", key, field, result)

	L.Push(lua.LTrue)
	L.Push(lua.LNil)
	L.Push(lua.LBool(result))
	return 3
}

func redisClientHDel(L *lua.LState) int {
	key := L.CheckString(1)
	field := L.CheckString(2)
	if "" == key || "" == field {
		L.Push(lua.LFalse)
		L.Push(lua.LString("参数key和field均不能为空"))
		L.Push(lua.LNil)
		return 3
	}

	cc, err := getCompositeClient(L)
	if err != nil {
		log.Warnf("获取Redis CompositeClient错误[%s]", err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(lua.LNil)
		return 3
	}

	var result int64
	result, err = cc.HDel(key, field)
	if err != nil {
		log.Warnf("Redis HDel: Key[%s]、Field[%s]、异常[%s]", key, field, err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(lua.LNil)
		return 3
	}

	log.Debugf("Redis RPush: Key[%s]、Field[%s]、结果[%d]", key, field, result)

	L.Push(lua.LTrue)
	L.Push(lua.LNil)
	L.Push(lua.LNumber(result))
	return 3
}

func redisClientRPush(L *lua.LState) int {
	key := L.CheckString(1)
	value := L.CheckAny(2)
	if "" == key || nil == value {
		L.Push(lua.LFalse)
		L.Push(lua.LString("参数key和value均不能为空"))
		L.Push(lua.LNil)
		return 3
	}

	cc, err := getCompositeClient(L)
	if err != nil {
		log.Warnf("获取Redis CompositeClient错误[%s]", err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(lua.LNil)
		return 3
	}

	var result int64
	result, err = cc.RPush(key, luautils.LvToInterface(value, true))
	if err != nil {
		log.Warnf("Redis RPush: Key[%s]、异常[%s]", key, err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(lua.LNil)
		return 3
	}

	log.Debugf("Redis RPush: Key[%s]、结果[%d]", key, result)

	L.Push(lua.LTrue)
	L.Push(lua.LNil)
	L.Push(lua.LNumber(result))
	return 3
}

func redisClientLRem(L *lua.LState) int {
	key := L.CheckString(1)
	value := L.CheckAny(2)
	if "" == key || nil == value {
		L.Push(lua.LFalse)
		L.Push(lua.LString("参数key和value均不能为空"))
		L.Push(lua.LNil)
		return 3
	}

	cc, err := getCompositeClient(L)
	if err != nil {
		log.Warnf("获取Redis CompositeClient错误[%s]", err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(lua.LNil)
		return 3
	}

	var result int64
	result, err = cc.LRem(key, luautils.LvToInterface(value, true))
	if err != nil {
		log.Warnf("Redis LRem: Key[%s]、异常[%s]", key, err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(lua.LNil)
		return 3
	}

	log.Debugf("Redis LRem: Key[%s]、结果[%d]", key, result)

	L.Push(lua.LTrue)
	L.Push(lua.LNil)
	L.Push(lua.LNumber(result))
	return 3
}

func redisClientSAdd(L *lua.LState) int {
	key := L.CheckString(1)
	value := L.CheckAny(2)
	if "" == key || nil == value {
		L.Push(lua.LFalse)
		L.Push(lua.LString("参数key和value均不能为空"))
		L.Push(lua.LNil)
		return 3
	}

	cc, err := getCompositeClient(L)
	if err != nil {
		log.Warnf("获取Redis CompositeClient错误[%s]", err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(lua.LNil)
		return 3
	}

	var result int64
	result, err = cc.SAdd(key, luautils.LvToInterface(value, true))
	if err != nil {
		log.Warnf("Redis SAdd: Key[%s]、异常[%s]", key, err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(lua.LNil)
		return 3
	}

	log.Debugf("Redis SAdd: Key[%s]、结果[%d]", key, result)

	L.Push(lua.LTrue)
	L.Push(lua.LNil)
	L.Push(lua.LNumber(result))
	return 3
}

func redisClientSRem(L *lua.LState) int {
	key := L.CheckString(1)
	value := L.CheckAny(2)
	if "" == key || nil == value {
		L.Push(lua.LFalse)
		L.Push(lua.LString("参数key和value均不能为空"))
		L.Push(lua.LNil)
		return 3
	}

	cc, err := getCompositeClient(L)
	if err != nil {
		log.Warnf("获取Redis CompositeClient错误[%s]", err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(lua.LNil)
		return 3
	}

	var result int64
	result, err = cc.SRem(key, luautils.LvToInterface(value, true))
	if err != nil {
		log.Warnf("Redis SRem: Key[%s]、异常[%s]", key, err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(lua.LNil)
		return 3
	}

	log.Debugf("Redis SRem: Key[%s]、结果[%d]", key, result)

	L.Push(lua.LTrue)
	L.Push(lua.LNil)
	L.Push(lua.LNumber(result))
	return 3
}

func redisClientZAdd(L *lua.LState) int {
	key := L.CheckString(1)
	score := L.CheckNumber(2)
	value := L.CheckAny(3)
	if "" == key || nil == value {
		L.Push(lua.LFalse)
		L.Push(lua.LString("参数key和value均不能为空"))
		L.Push(lua.LNil)
		return 3
	}

	cc, err := getCompositeClient(L)
	if err != nil {
		log.Warnf("获取Redis CompositeClient错误[%s]", err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(lua.LNil)
		return 3
	}

	var result int64
	result, err = cc.ZAdd(key, float64(lua.LVAsNumber(score)), luautils.LvToInterface(value, true))
	if err != nil {
		log.Warnf("Redis ZAdd: Key[%s]、Score[%v]、异常[%s]", key, float64(lua.LVAsNumber(score)), err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(lua.LNil)
		return 3
	}

	log.Debugf("Redis ZAdd: Key[%s]、结果[%d]", key, result)

	L.Push(lua.LTrue)
	L.Push(lua.LNil)
	L.Push(lua.LNumber(result))
	return 3
}

func redisClientZRem(L *lua.LState) int {
	key := L.CheckString(1)
	value := L.CheckAny(3)
	if "" == key || nil == value {
		L.Push(lua.LFalse)
		L.Push(lua.LString("参数key和value均不能为空"))
		L.Push(lua.LNil)
		return 3
	}

	cc, err := getCompositeClient(L)
	if err != nil {
		log.Warnf("获取Redis CompositeClient错误[%s]", err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(lua.LNil)
		return 3
	}

	var result int64
	result, err = cc.ZRem(key, luautils.LvToInterface(value, true))
	if err != nil {
		log.Warnf("Redis ZRem: Key[%s]、异常[%s]", key, err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(lua.LNil)
		return 3
	}

	log.Debugf("Redis ZRem: Key[%s]、结果[%d]", key, result)

	L.Push(lua.LTrue)
	L.Push(lua.LNil)
	L.Push(lua.LNumber(result))
	return 3
}

func redisClientExpire(L *lua.LState) int {
	key := L.CheckString(1)
	expiration := L.CheckNumber(2)
	if "" == key {
		L.Push(lua.LFalse)
		L.Push(lua.LString("参数key不能为空"))
		L.Push(lua.LNil)
		return 3
	}

	cc, err := getCompositeClient(L)
	if err != nil {
		log.Warnf("获取Redis CompositeClient错误[%s]", err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(lua.LNil)
		return 3
	}

	var result bool
	result, err = cc.Expire(key, time.Duration(float64(lua.LVAsNumber(expiration))))
	if err != nil {
		log.Warnf("Redis Expire: Key[%s]、异常[%s]", key, err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(lua.LNil)
		return 3
	}

	log.Debugf("Redis Expire: Key[%s]、结果[%v]", key, result)

	L.Push(lua.LTrue)
	L.Push(lua.LNil)
	L.Push(lua.LBool(result))
	return 3
}

func redisClientGet(L *lua.LState) int {
	key := L.CheckString(1)
	if "" == key {
		L.Push(lua.LFalse)
		L.Push(lua.LString("参数key不能为空"))
		L.Push(lua.LNil)
		return 3
	}

	cc, err := getCompositeClient(L)
	if err != nil {
		log.Warnf("获取Redis CompositeClient错误[%s]", err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(lua.LNil)
		return 3
	}

	var result string
	result, err = cc.Get(key)
	if err != nil {
		log.Warnf("Redis Get: Key[%s]、异常[%s]", key, err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(lua.LNil)
		return 3
	}

	log.Debugf("Redis Get: Key[%s]、结果[%s]", key, result)

	if "" == result {
		L.Push(lua.LTrue)
		L.Push(lua.LNil)
		L.Push(lua.LString(""))
		return 3
	}

	if stringutils.IsNumber(result) {
		vv, _ := strconv.ParseFloat(result, 64)
		L.Push(lua.LTrue)
		L.Push(lua.LNil)
		L.Push(lua.LNumber(vv))
		return 3
	}

	vos := make(map[string]interface{})
	err = json.Unmarshal(byteutils.StringToBytes(result), &vos)
	if err != nil {
		log.Warn(err.Error())
		L.Push(lua.LTrue)
		L.Push(lua.LNil)
		L.Push(lua.LString(result))
		return 3
	}

	table := L.NewTable()
	luautils.PaddingLuaTableWithMap(L, table, vos)
	L.Push(lua.LTrue)
	L.Push(lua.LNil)
	L.Push(table)
	return 3
}

func redisClientIncr(L *lua.LState) int {
	key := L.CheckString(1)
	if "" == key {
		L.Push(lua.LFalse)
		L.Push(lua.LString("参数key不能为空"))
		L.Push(lua.LNil)
		return 3
	}

	cc, err := getCompositeClient(L)
	if err != nil {
		log.Warnf("获取Redis CompositeClient错误[%s]", err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(lua.LNil)
		return 3
	}

	var result int64
	result, err = cc.Incr(key)
	if err != nil {
		log.Warnf("Redis Incr: Key[%s]、异常[%s]", key, err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(lua.LNil)
		return 3
	}

	log.Debugf("Redis Incr: Key[%s]、结果[%d]", key, result)

	L.Push(lua.LTrue)
	L.Push(lua.LNil)
	L.Push(lua.LNumber(result))
	return 3
}

func redisClientDecr(L *lua.LState) int {
	key := L.CheckString(1)
	if "" == key {
		L.Push(lua.LFalse)
		L.Push(lua.LString("参数key不能为空"))
		L.Push(lua.LNil)
		return 3
	}

	cc, err := getCompositeClient(L)
	if err != nil {
		log.Warnf("获取Redis CompositeClient错误[%s]", err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(lua.LNil)
		return 3
	}

	var result int64
	result, err = cc.Decr(key)
	if err != nil {
		log.Warnf("Redis Decr: Key[%s]、异常[%s]", key, err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(lua.LNil)
		return 3
	}

	log.Debugf("Redis Decr: Key[%s]、结果[%d]", key, result)

	L.Push(lua.LTrue)
	L.Push(lua.LNil)
	L.Push(lua.LNumber(result))
	return 3
}

func redisClientAppend(L *lua.LState) int {
	key := L.CheckString(1)
	value := L.CheckString(2)
	if "" == key || "" == value {
		L.Push(lua.LFalse)
		L.Push(lua.LString("参数key和value均不能为空"))
		L.Push(lua.LNil)
		return 3
	}

	cc, err := getCompositeClient(L)
	if err != nil {
		log.Warnf("获取Redis CompositeClient错误[%s]", err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(lua.LNil)
		return 3
	}

	var result int64
	result, err = cc.Append(key, value)
	if err != nil {
		log.Warnf("Redis Append: Key[%s]、异常[%s]", key, err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(lua.LNil)
		return 3
	}

	log.Debugf("Redis HExists: Key[%s]、结果[%d]", key, result)

	L.Push(lua.LTrue)
	L.Push(lua.LNil)
	L.Push(lua.LNumber(result))
	return 3
}

func redisClientHGet(L *lua.LState) int {
	key := L.CheckString(1)
	field := L.CheckString(2)
	if "" == key || "" == field {
		L.Push(lua.LFalse)
		L.Push(lua.LString("参数key和field均不能为空"))
		L.Push(lua.LNil)
		return 3
	}

	cc, err := getCompositeClient(L)
	if err != nil {
		log.Warnf("获取Redis CompositeClient错误[%s]", err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(lua.LNil)
		return 3
	}

	var result string
	result, err = cc.HGet(key, field)
	if err != nil {
		log.Warnf("Redis HGet: Key[%s]、Field[%s]、异常[%s]", key, field, err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(lua.LNil)
		return 3
	}

	log.Debugf("Redis HGet: Key[%s]、Field[%s]、结果[%s]", key, field, result)

	if stringutils.IsNumber(result) {
		vv, _ := strconv.ParseFloat(result, 64)
		L.Push(lua.LTrue)
		L.Push(lua.LNil)
		L.Push(lua.LNumber(vv))
		return 3
	}

	vos := make(map[string]interface{})
	err = json.Unmarshal(byteutils.StringToBytes(result), &vos)
	if err != nil {
		L.Push(lua.LTrue)
		L.Push(lua.LNil)
		L.Push(lua.LString(result))
		return 3
	}

	table := L.NewTable()
	luautils.PaddingLuaTableWithMap(L, table, vos)
	L.Push(lua.LTrue)
	L.Push(lua.LNil)
	L.Push(table)
	return 3
}

func redisClientHExists(L *lua.LState) int {
	key := L.CheckString(1)
	field := L.CheckString(2)
	if "" == key || "" == field {
		L.Push(lua.LFalse)
		L.Push(lua.LString("参数key和field均不能为空"))
		L.Push(lua.LNil)
		return 3
	}

	cc, err := getCompositeClient(L)
	if err != nil {
		log.Warnf("获取Redis CompositeClient错误[%s]", err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(lua.LNil)
		return 3
	}

	var result bool
	result, err = cc.HExists(key, field)
	if err != nil {
		log.Warnf("Redis HExists: Key[%s]、Field[%s]、异常[%s]", key, field, err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(lua.LNil)
		return 3
	}

	log.Debugf("Redis HExists: Key[%s]、Field[%s]、结果[%v]", key, field, result)

	L.Push(lua.LTrue)
	L.Push(lua.LNil)
	L.Push(lua.LBool(result))
	return 3
}

func getCompositeClient(L *lua.LState) (*redis.CompositeClient, error) {
	temp := L.GetGlobal(constants.EndpointKey)
	if nil == temp {
		return nil, errors.Errorf("Lua上下文中没有EndpointKey属性")
	}
	endpointKey := luautils.LvToString(temp)

	var compositeClient *redis.CompositeClient
	var exist bool
	_lockOfCompositeClients.RLock()
	compositeClient, exist = _compositeClients[endpointKey]
	_lockOfCompositeClients.RUnlock()
	if exist {
		return compositeClient, nil
	}

	return createCompositeClient(endpointKey)
}

func createCompositeClient(endpointKey string) (*redis.CompositeClient, error) {
	_lockOfCompositeClients.Lock()
	defer _lockOfCompositeClients.Unlock()

	endpointId := stringutils.ToUint64Safe(endpointKey)
	endpointInfo, err := dao.GetEndpointInfoDao().Get(endpointId)
	if nil != err {
		return nil, err
	}
	if nil == endpointInfo {
		return nil, errors.Errorf("无法获取Id为[%d]的Endpoint信息", endpointId)
	}

	compositeClient := redis.NewCompositeClient(endpointInfo)
	err = compositeClient.Connect()
	if nil != err {
		return nil, err
	}
	err = compositeClient.Ping()
	if nil != err {
		return nil, err
	}
	_compositeClients[endpointKey] = compositeClient

	log.Infof("创建Redis客户端,CompositeClients Length[%d]", len(_compositeClients))

	return compositeClient, err
}

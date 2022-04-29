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

package redis

import (
	"bytes"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/yuin/gopher-lua"

	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/endpoint/luaengine"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/stringutils"
)

type Endpoint struct {
	info          *po.EndpointInfo
	singleClient  *redis.Client
	clusterClient *redis.ClusterClient
}

func NewEndpoint(info *po.EndpointInfo) *Endpoint {
	return &Endpoint{
		info: info,
	}
}

func (s *Endpoint) Connect() error {
	addrs := strings.Split(s.info.GetAddresses(), stringutils.Comma)
	if len(addrs) > 1 {
		if s.info.GroupType == constants.RedisGroupTypeSentinel {
			client := redis.NewFailoverClient(&redis.FailoverOptions{
				MasterName:    s.info.MasterName,
				SentinelAddrs: addrs,
				Password:      s.info.Password,
				DB:            int(s.info.Database),
			})
			s.singleClient = client
		}

		if s.info.GroupType == constants.RedisGroupTypeCluster {
			client := redis.NewClusterClient(&redis.ClusterOptions{
				Addrs:    addrs,
				Password: s.info.Password,
			})
			s.clusterClient = client
		}
	} else {
		client := redis.NewClient(&redis.Options{
			Addr:     s.info.GetAddresses(),
			Password: s.info.Password,
			DB:       int(s.info.Database),
		})
		s.singleClient = client
	}

	if s.singleClient == nil && s.clusterClient == nil {
		return errors.Errorf("Redis客户端创建失败")
	}

	return s.Ping()
}

func (s *Endpoint) Ping() error {
	var err error
	if s.singleClient != nil {
		_, err = s.singleClient.Ping().Result()
	}
	if s.clusterClient != nil {
		_, err = s.clusterClient.Ping().Result()
	}
	return err
}

func (s *Endpoint) Close() {
	if s.singleClient != nil {
		s.singleClient.Close()
	}
	if s.clusterClient != nil {
		s.clusterClient.Close()
	}
}

func (s *Endpoint) createPipeline() redis.Pipeliner {
	var pipe redis.Pipeliner
	if s.singleClient != nil {
		pipe = s.singleClient.Pipeline()
	}
	if s.clusterClient != nil {
		pipe = s.clusterClient.Pipeline()
	}
	return pipe
}

func (s *Endpoint) encodeKey(raw *bo.RowEventRequest, rule *po.Rule, ctx *bo.RuleContext) (string, error) {
	if constants.RedisStructureString != rule.GetRedisStructure() {
		return rule.RedisKeyFixValue, nil
	}

	if constants.RedisKeyBuilderColumnValue == rule.GetRedisKeyBuilder() {
		index := ctx.GetTableColumnIndex(rule.GetRedisKeyColumn())
		if index < 0 {
			return "", errors.Errorf("[%s] Redis Key列[%s],不在表结构中", ctx.GetPipelineName(), rule.GetRedisKeyColumn())
		}
		key := stringutils.ToString(raw.Data[index])
		if rule.GetRedisKeyPrefix() != "" {
			key = rule.GetRedisKeyPrefix() + key
		}
		return key, nil
	}

	if constants.RedisKeyBuilderExpression == rule.GetRedisKeyBuilder() {
		kv := ctx.GetRow(raw)
		var tmplBytes bytes.Buffer
		err := ctx.GetRedisKeyExpressionTmpl().Execute(&tmplBytes, kv)
		if err != nil {
			return "", err
		}
		return tmplBytes.String(), nil
	}

	return "", errors.New("请先设置KEY生成方式")
}

func (s *Endpoint) encodeHashField(raw *bo.RowEventRequest, rule *po.Rule, ctx *bo.RuleContext) (string, error) {
	index := ctx.GetTableColumnIndex(rule.GetRedisHashFieldColumn())
	if index < 0 {
		return "", errors.Errorf("[%] Redis Field列[%s],不在表结构中", ctx.GetPipelineName(), rule.GetRedisKeyColumn())
	}
	field := stringutils.ToString(raw.Data[index])
	if rule.GetRedisHashFieldPrefix() != "" {
		field = rule.GetRedisHashFieldPrefix() + field
	}
	return field, nil
}

func (s *Endpoint) encodeScoreField(raw *bo.RowEventRequest, rule *po.Rule, ctx *bo.RuleContext) (float64, error) {
	index := ctx.GetTableColumnIndex(rule.GetRedisSortedSetScoreColumn())
	if index < 0 {
		return 0, errors.Errorf("[%] Redis Score列[%s],不在表结构中", ctx.GetPipelineName(), rule.GetRedisKeyColumn())
	}
	vv := raw.Data[index]
	str := stringutils.ToString(vv)
	score, err := strconv.ParseFloat(str, 64)
	if nil != err {
		return 0, errors.Errorf("[%] Redis Score列[%s],必须是数字类型", ctx.GetPipelineName(), rule.GetRedisKeyColumn())
	}
	return score, nil
}

func (s *Endpoint) parseByRegular(request *bo.RowEventRequest, ctx *bo.RuleContext, pipeline redis.Pipeliner) error {
	rule := ctx.GetRule()
	key, err := s.encodeKey(request, rule, ctx)
	if err != nil {
		return err
	}

	var value string
	switch rule.GetRedisStructure() {
	case constants.RedisStructureString:
		if request.Action == canal.DeleteAction {
			pipeline.Del(key)
		} else {
			value, err = ctx.EncodeValue(request)
			if err != nil {
				return err
			}
			pipeline.Set(key, value, 0)
		}
		log.Infof("管道[%s] 接收端[redis]、Structure[String]、事件[%s]、KEY[%s]", ctx.GetPipelineName(), request.Action, key)
	case constants.RedisStructureHash:
		var field string
		field, err = s.encodeHashField(request, rule, ctx)
		if err != nil {
			return err
		}
		if request.Action == canal.DeleteAction {
			pipeline.HDel(key, field)
		} else {
			value, err = ctx.EncodeValue(request)
			if err != nil {
				return err
			}
			pipeline.HSet(key, field, value)
		}
		log.Infof("管道[%s] 接收端[redis]、Structure[Hash]、事件[%s]、KEY[%s]、FIELD[%s]", ctx.GetPipelineName(), request.Action, key, field)
	case constants.RedisStructureList:
		value, err = ctx.EncodeValue(request)
		if err != nil {
			return err
		}
		if request.Action == canal.DeleteAction {
			pipeline.LRem(key, 0, value)
		} else if request.Action == canal.UpdateAction {
			var preValue string
			preValue, err = ctx.EncodePreValue(request)
			if err != nil {
				return err
			}
			pipeline.LRem(key, 0, preValue)
			pipeline.RPush(key, value)
		} else {
			pipeline.RPush(key, value)
		}
		log.Infof("管道[%s] 接收端[redis]、Structure[List]、事件[%s]、KEY[%s]", ctx.GetPipelineName(), request.Action, key)
	case constants.RedisStructureSet:
		value, err = ctx.EncodeValue(request)
		if err != nil {
			return err
		}
		if request.Action == canal.DeleteAction {
			pipeline.SRem(key, value)
		} else if request.Action == canal.UpdateAction {
			var preValue string
			preValue, err = ctx.EncodePreValue(request)
			if err != nil {
				return err
			}
			pipeline.SRem(key, 0, preValue) //移除集中之前的数据
			pipeline.SAdd(key, value)
		} else {
			pipeline.SAdd(key, value)
		}
		log.Infof("管道[%s] 接收端[redis]、Structure[Set]、事件[%s]、KEY[%s]", ctx.GetPipelineName(), request.Action, key)
	case constants.RedisStructureSortedSet:
		value, err = ctx.EncodeValue(request)
		if err != nil {
			return err
		}
		if request.Action == canal.DeleteAction {
			pipeline.ZRem(key, value)
		} else if request.Action == canal.UpdateAction {
			var preValue string
			var score float64
			preValue, err = ctx.EncodePreValue(request)
			if err != nil {
				return err
			}
			score, err = s.encodeScoreField(request, rule, ctx)
			if err != nil {
				return err
			}
			pipeline.ZRem(key, 0, preValue) //移除有序集中之前的数据
			val := redis.Z{Score: score, Member: value}
			pipeline.ZAdd(key, val)
		} else {
			var score float64
			score, err = s.encodeScoreField(request, rule, ctx)
			if err != nil {
				return err
			}
			val := redis.Z{Score: score, Member: value}
			pipeline.ZAdd(key, val)
		}
		log.Infof("管道[%s] 接收端[redis]、Structure[SortedSet]、事件[%s]、KEY[%s]", ctx.GetPipelineName(), request.Action, key)
	}

	return nil
}

func (s *Endpoint) parseByLua(request *bo.RowEventRequest, ctx *bo.RuleContext, pipeline redis.Pipeliner, lvm *lua.LState) error {
	var L *lua.LState
	if lvm != nil {
		L = lvm
	} else {
		L = ctx.GetLuaVM()
	}

	event := L.NewTable()
	row := L.NewTable()
	luaengine.PaddingLuaTableWithMap(L, row, ctx.GetRow(request))
	L.SetTable(event, luaengine.RowKey, row)
	if canal.UpdateAction == request.Action {
		preRow := L.NewTable()
		luaengine.PaddingLuaTableWithMap(L, preRow, ctx.GetPreRow(request))
		L.SetTable(event, luaengine.PreRowKey, preRow)
	}
	L.SetTable(event, luaengine.ActionKey, lua.LString(request.Action))

	result := L.NewTable()
	L.SetGlobal(luaengine.GlobalVariableResult, result)

	err := L.CallByParam(lua.P{
		Fn:      L.GetGlobal(luaengine.HandleFunctionName),
		NRet:    0,
		Protect: true,
	}, event)
	if err != nil {
		log.Errorf("管道[%s]，表[%s]的Lua脚本执行错误[%s]", ctx.GetPipelineName(), ctx.GetTableFullName(), err.Error)
		return constants.LuaScriptError
	}

	result.ForEach(func(k lua.LValue, v lua.LValue) {
		combine := luaengine.LvToString(k)
		clen := len(combine)
		action := combine[0:6]
		if constants.ExpireAction == action {
			key := combine[7:clen]
			value := stringutils.ToInt64Safe(luaengine.LvToString(v))
			expiration := time.Duration(value)
			pipeline.Expire(key, expiration*time.Second)
			return
		}

		structure, _ := strconv.Atoi(combine[7:8])
		switch structure {
		case constants.RedisStructureString:
			key := combine[9:clen]
			if action == canal.DeleteAction {
				pipeline.Del(key)
			} else {
				value := luaengine.LvToInterface(v, true)
				pipeline.Set(key, value, 0)
			}
			log.Infof("管道[%s] 接收端[redis]、Structure[String]、事件[%s]、KEY[%s]", ctx.GetPipelineName(), request.Action, key)
		case constants.RedisStructureList:
			key := combine[9:clen]
			value := luaengine.LvToInterface(v, true)
			if action == canal.DeleteAction {
				pipeline.LRem(key, 0, value)
			} else {
				pipeline.RPush(key, value)
			}
			log.Infof("管道[%s] 接收端[redis]、Structure[List]、事件[%s]、KEY[%s]", ctx.GetPipelineName(), request.Action, key)
		case constants.RedisStructureSet:
			key := combine[9:clen]
			value := luaengine.LvToInterface(v, true)
			if action == canal.DeleteAction {
				pipeline.SRem(key, 0, value)
			} else {
				pipeline.SAdd(key, value)
			}
			log.Infof("管道[%s] 接收端[redis]、Structure[Set]、事件[%s]、KEY[%s]", ctx.GetPipelineName(), request.Action, key)
		case constants.RedisStructureHash:
			luaKey := L.GetTable(v, lua.LString("key"))
			luaField := L.GetTable(v, lua.LString("field"))
			key := luaKey.String()
			field := luaengine.LvToString(luaField)
			if action == canal.DeleteAction {
				pipeline.HDel(key, field)
			} else {
				luaValue := L.GetTable(v, lua.LString("value"))
				value := luaengine.LvToInterface(luaValue, true)
				pipeline.HSet(key, field, value)
			}
			log.Infof("管道[%s] 接收端[redis]、Structure[Hash]、事件[%s]、KEY[%s]、FIELD[%s]", ctx.GetPipelineName(), action, key, field)
		case constants.RedisStructureSortedSet:
			luaKey := L.GetTable(v, lua.LString("key"))
			luaValue := L.GetTable(v, lua.LString("value"))
			key := luaKey.String()
			value := luaengine.LvToInterface(luaValue, true)
			if action == canal.DeleteAction {
				pipeline.ZRem(key, value)
			} else {
				luaScore := luaengine.LvToString(L.GetTable(v, lua.LString("score")))
				score := stringutils.ToFloat64Safe(luaScore)
				z := redis.Z{Score: score, Member: value}
				pipeline.ZAdd(key, z)
			}
			log.Infof("管道[%s] 接收端[redis]、Structure[SortedSet]、事件[%s]、KEY[%s]、FIELD[%s]", ctx.GetPipelineName(), action, key)
		}
	})

	return nil
}

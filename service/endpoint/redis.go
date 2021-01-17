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
package endpoint

import (
	"bytes"
	"log"
	"strings"
	"sync"

	"github.com/go-redis/redis"
	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"

	"go-mysql-transfer/global"
	"go-mysql-transfer/metrics"
	"go-mysql-transfer/model"
	"go-mysql-transfer/service/luaengine"
	"go-mysql-transfer/util/logs"
	"go-mysql-transfer/util/stringutil"
)

type RedisEndpoint struct {
	isCluster bool
	client    *redis.Client
	cluster   *redis.ClusterClient
	retryLock sync.Mutex
}

func newRedisEndpoint() *RedisEndpoint {
	cfg := global.Cfg()
	r := &RedisEndpoint{}

	list := strings.Split(cfg.RedisAddr, ",")
	if len(list) == 1 {
		r.client = redis.NewClient(&redis.Options{
			Addr:     cfg.RedisAddr,
			Password: cfg.RedisPass,
			DB:       cfg.RedisDatabase,
		})
	} else {
		if cfg.RedisGroupType == global.RedisGroupTypeSentinel {
			r.client = redis.NewFailoverClient(&redis.FailoverOptions{
				MasterName:    cfg.RedisMasterName,
				SentinelAddrs: list,
				Password:      cfg.RedisPass,
				DB:            cfg.RedisDatabase,
			})
		}
		if cfg.RedisGroupType == global.RedisGroupTypeCluster {
			r.isCluster = true
			r.cluster = redis.NewClusterClient(&redis.ClusterOptions{
				Addrs:    list,
				Password: cfg.RedisPass,
			})
		}
	}

	return r
}

func (s *RedisEndpoint) Connect() error {
	return s.Ping()
}

func (s *RedisEndpoint) Ping() error {
	var err error
	if s.isCluster {
		_, err = s.cluster.Ping().Result()
	} else {
		_, err = s.client.Ping().Result()
	}
	return err
}

func (s *RedisEndpoint) pipe() redis.Pipeliner {
	var pipe redis.Pipeliner
	if s.isCluster {
		pipe = s.cluster.Pipeline()
	} else {
		pipe = s.client.Pipeline()
	}
	return pipe
}

func (s *RedisEndpoint) Consume(from mysql.Position, rows []*model.RowRequest) error {
	pipe := s.pipe()
	for _, row := range rows {
		rule, _ := global.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		metrics.UpdateActionNum(row.Action, row.RuleKey)

		if rule.LuaEnable() {
			var err error
			var ls []*model.RedisRespond
			kvm := rowMap(row, rule, true)
			if row.Action == canal.UpdateAction {
				previous := oldRowMap(row, rule, true)
				ls, err = luaengine.DoRedisOps(kvm, previous, row.Action, rule)
			} else {
				ls, err = luaengine.DoRedisOps(kvm, nil, row.Action, rule)
			}
			if err != nil {
				log.Println("Lua 脚本执行失败!!! ,详情请参见日志")
				return errors.Errorf("Lua 脚本执行失败 : %s ", errors.ErrorStack(err))
			}
			for _, resp := range ls {
				s.preparePipe(resp, pipe)
				logs.Infof("action: %s, structure: %s ,key: %s ,field: %s, value: %v", resp.Action, resp.Structure, resp.Key, resp.Field, resp.Val)
			}
			kvm = nil
		} else {
			resp := s.ruleRespond(row, rule)
			s.preparePipe(resp, pipe)
			logs.Infof("action: %s, structure: %s ,key: %s ,field: %s, value: %v", resp.Action, resp.Structure, resp.Key, resp.Field, resp.Val)
		}
	}

	_, err := pipe.Exec()
	if err != nil {
		return err
	}

	logs.Infof("处理完成 %d 条数据", len(rows))
	return nil
}

func (s *RedisEndpoint) Stock(rows []*model.RowRequest) int64 {
	pipe := s.pipe()
	for _, row := range rows {
		rule, _ := global.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		if rule.LuaEnable() {
			kvm := rowMap(row, rule, true)
			ls, err := luaengine.DoRedisOps(kvm, nil, row.Action, rule)
			if err != nil {
				logs.Errorf("lua 脚本执行失败 : %s ", errors.ErrorStack(err))
				break
			}
			for _, resp := range ls {
				s.preparePipe(resp, pipe)
			}
		} else {
			resp := s.ruleRespond(row, rule)
			resp.Action = row.Action
			resp.Structure = rule.RedisStructure
			s.preparePipe(resp, pipe)
		}
	}

	var counter int64
	res, err := pipe.Exec()
	if err != nil {
		logs.Error(err.Error())
	}

	for _, re := range res {
		if re.Err() == nil {
			counter++
		}
	}

	return counter
}

func (s *RedisEndpoint) ruleRespond(row *model.RowRequest, rule *global.Rule) *model.RedisRespond {
	resp := new(model.RedisRespond)
	resp.Action = row.Action
	resp.Structure = rule.RedisStructure

	kvm := rowMap(row, rule, false)
	resp.Key = s.encodeKey(row, rule)
	if resp.Structure == global.RedisStructureHash {
		resp.Field = s.encodeHashField(row, rule)
	}
	if resp.Structure == global.RedisStructureSortedSet {
		resp.Score = s.encodeSortedSetScoreField(row, rule)
	}

	if resp.Action == canal.InsertAction {
		resp.Val = encodeValue(rule, kvm)
	} else if resp.Action == canal.UpdateAction {
		if rule.RedisStructure == global.RedisStructureList ||
			rule.RedisStructure == global.RedisStructureSet ||
			rule.RedisStructure == global.RedisStructureSortedSet {
			oldKvm := oldRowMap(row, rule, false)
			resp.OldVal = encodeValue(rule, oldKvm)
		}
		resp.Val = encodeValue(rule, kvm)
	} else {
		if rule.RedisStructure == global.RedisStructureList ||
			rule.RedisStructure == global.RedisStructureSet ||
			rule.RedisStructure == global.RedisStructureSortedSet {
			resp.Val = encodeValue(rule, kvm)
		}
	}

	return resp
}

func (s *RedisEndpoint) preparePipe(resp *model.RedisRespond, pipe redis.Cmdable) {
	switch resp.Structure {
	case global.RedisStructureString:
		if resp.Action == canal.DeleteAction {
			pipe.Del(resp.Key)
		} else {
			pipe.Set(resp.Key, resp.Val, 0)
		}
	case global.RedisStructureHash:
		if resp.Action == canal.DeleteAction {
			pipe.HDel(resp.Key, resp.Field)
		} else {
			pipe.HSet(resp.Key, resp.Field, resp.Val)
		}
	case global.RedisStructureList:
		if resp.Action == canal.DeleteAction {
			pipe.LRem(resp.Key, 0, resp.Val)
		} else if resp.Action == canal.UpdateAction {
			pipe.LRem(resp.Key, 0, resp.OldVal)
			pipe.RPush(resp.Key, resp.Val)
		} else {
			pipe.RPush(resp.Key, resp.Val)
		}
	case global.RedisStructureSet:
		if resp.Action == canal.DeleteAction {
			pipe.SRem(resp.Key, resp.Val)
		} else if resp.Action == canal.UpdateAction {
			pipe.SRem(resp.Key, 0, resp.OldVal)
			pipe.SAdd(resp.Key, resp.Val)
		} else {
			pipe.SAdd(resp.Key, resp.Val)
		}
	case global.RedisStructureSortedSet:
		if resp.Action == canal.DeleteAction {
			pipe.ZRem(resp.Key, resp.Val)
		} else if resp.Action == canal.UpdateAction {
			pipe.ZRem(resp.Key, 0, resp.OldVal)
			val := redis.Z{Score: resp.Score, Member: resp.Val}
			pipe.ZAdd(resp.Key, val)
		} else {
			val := redis.Z{Score: resp.Score, Member: resp.Val}
			pipe.ZAdd(resp.Key, val)
		}
	}
}

func (s *RedisEndpoint) encodeKey(req *model.RowRequest, rule *global.Rule) string {
	if rule.RedisKeyValue != "" {
		return rule.RedisKeyValue
	}

	if rule.RedisKeyFormatter != "" {
		kv := rowMap(req, rule, true)
		var tmplBytes bytes.Buffer
		err := rule.RedisKeyTmpl.Execute(&tmplBytes, kv)
		if err != nil {
			return ""
		}
		return tmplBytes.String()
	}

	var key string
	if rule.RedisKeyColumnIndex < 0 {
		for _, v := range rule.RedisKeyColumnIndexs {
			key += stringutil.ToString(req.Row[v])
		}
	} else {
		key = stringutil.ToString(req.Row[rule.RedisKeyColumnIndex])
	}
	if rule.RedisKeyPrefix != "" {
		key = rule.RedisKeyPrefix + key
	}

	return key
}

func (s *RedisEndpoint) encodeHashField(req *model.RowRequest, rule *global.Rule) string {
	var field string

	if rule.RedisHashFieldColumnIndex < 0 {
		for _, v := range rule.RedisHashFieldColumnIndexs {
			field += stringutil.ToString(req.Row[v])
		}
	} else {
		field = stringutil.ToString(req.Row[rule.RedisHashFieldColumnIndex])
	}

	if rule.RedisHashFieldPrefix != "" {
		field = rule.RedisHashFieldPrefix + field
	}

	return field
}

func (s *RedisEndpoint) encodeSortedSetScoreField(req *model.RowRequest, rule *global.Rule) float64 {
	obj := req.Row[rule.RedisHashFieldColumnIndex]
	if obj == nil {
		return 0
	}

	str := stringutil.ToString(obj)
	return stringutil.ToFloat64Safe(str)
}

func (s *RedisEndpoint) Close() {
	if s.client != nil {
		s.client.Close()
	}
}

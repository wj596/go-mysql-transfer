package endpoint

import (
	"strings"
	"time"

	"github.com/go-redis/redis"
	"github.com/vmihailenco/msgpack"

	"go-mysql-transfer/global"
	"go-mysql-transfer/service/luaengine"
	"go-mysql-transfer/util/logutil"
	"go-mysql-transfer/util/stringutil"
)

type RedisEndpoint struct {
	isCluster bool
	client    *redis.Client
	cluster   *redis.ClusterClient
}

func newRedisEndpoint(c *global.Config) *RedisEndpoint {
	r := &RedisEndpoint{}
	list := strings.Split(c.RedisAddr, ",")
	if len(list) == 1 {
		r.client = redis.NewClient(&redis.Options{
			Addr:     c.RedisAddr,
			Password: c.RedisPass,
			DB:       c.RedisDatabase,
		})
	} else {
		if c.RedisGroupType == global.RedisGroupTypeSentinel {
			r.client = redis.NewFailoverClient(&redis.FailoverOptions{
				MasterName:    c.RedisMasterName,
				SentinelAddrs: list,
				Password:      c.RedisPass,
				DB:            c.RedisDatabase,
			})
		}
		if c.RedisGroupType == global.RedisGroupTypeCluster {
			r.isCluster = true
			r.cluster = redis.NewClusterClient(&redis.ClusterOptions{
				Addrs:    list,
				Password: c.RedisPass,
			})
		}
	}

	return r
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

func (s *RedisEndpoint) Consume(rows []*global.RowRequest) {
	pipe := s.pipe()

	for _, row := range rows {
		rule, ignore := ignoreRow(row.RuleKey, len(row.Row))
		if ignore {
			continue
		}

		switch row.Action {
		case global.InsertAction:
			global.IncInsertNum(row.RuleKey)
		case global.UpdateAction:
			global.IncUpdateNum(row.RuleKey)
		case global.DeleteAction:
			global.IncDeleteNum(row.RuleKey)
		}

		if rule.LuaNecessary() {
			s.doLuaConsume(row, rule, pipe)
		} else {
			s.doRuleConsume(row, rule, pipe)
		}
	}

	_, err := pipe.Exec()
	if err != nil {
		logutil.Error(err.Error())
		logutil.Infof("%d 条数据处理失败，插入重试队列", len(rows))
		saveFailedRows(rows)
	} else {
		logutil.Infof("处理完成 %d 条数据", len(rows))
	}
}

func (s *RedisEndpoint) Stock(rows []*global.RowRequest) int {
	pipe := s.pipe()

	for _, row := range rows {
		rule, ignore := ignoreRow(row.RuleKey, len(row.Row))
		if ignore {
			continue
		}

		if rule.LuaNecessary() {
			s.doLuaConsume(row, rule, pipe)
		} else {
			s.doRuleConsume(row, rule, pipe)
		}
	}

	var succeeds int
	res, err := pipe.Exec()
	if err != nil {
		logutil.Error(err.Error())
	}
	for _, re := range res {
		if re.Err() == nil {
			succeeds++
		}
	}

	return succeeds
}

func (s *RedisEndpoint) doLuaConsume(row *global.RowRequest, rule *global.Rule, cmd redis.Cmdable) {
	kvm := keyValueMap(row, rule)

	responds, err := luaengine.DoRedisOps(kvm, rule)
	if err != nil {
		logutil.Error(err.Error())
	}

	for _, resp := range responds {
		switch rule.RedisStructure {
		case global.RedisStructureString:
			if row.Action == global.DeleteAction {
				cmd.Del(resp.Key)
			} else {
				cmd.Set(resp.Key, resp.Val, 0)
			}
		case global.RedisStructureHash:
			field := s.encodeHashField(row, rule)
			if row.Action == global.DeleteAction {
				cmd.HDel(resp.Key, field)
			} else {
				cmd.HSet(resp.Key, field, resp.Val)
			}
		case global.RedisStructureList:
			if row.Action == global.DeleteAction {
				cmd.LRem(rule.RedisKeyValue, 0, resp.Val)
			} else {
				cmd.RPush(rule.RedisKeyValue, resp.Val)
			}
		case global.RedisStructureSet:
			if row.Action == global.DeleteAction {
				cmd.SRem(rule.RedisKeyValue, resp.Val)
			} else {
				cmd.SAdd(rule.RedisKeyValue, resp.Val)
			}
		}

		logutil.Infof("%s by lua : %v", row.RuleKey, resp)
	}
}

func (s *RedisEndpoint) doRuleConsume(row *global.RowRequest, rule *global.Rule, cmd redis.Cmdable) {
	kvm := keyValueMap(row, rule)

	key := s.encodeKey(row, rule)
	val := encodeStringValue(rule.ValueEncoder, kvm)
	switch rule.RedisStructure {
	case global.RedisStructureString:
		if row.Action == global.DeleteAction {
			cmd.Del(key)
		} else {
			cmd.Set(key, val, 0)
		}
	case global.RedisStructureHash:
		field := s.encodeHashField(row, rule)
		if row.Action == global.DeleteAction {
			cmd.HDel(key, field)
		} else {
			cmd.HSet(key, field, val)
		}
	case global.RedisStructureList:
		if row.Action == global.DeleteAction {
			cmd.LRem(rule.RedisKeyValue, 0, val)
		} else {
			cmd.RPush(rule.RedisKeyValue, val)
		}
	case global.RedisStructureSet:
		if row.Action == global.DeleteAction {
			cmd.SRem(rule.RedisKeyValue, val)
		} else {
			cmd.SAdd(rule.RedisKeyValue, val)
		}
	}
}

func (s *RedisEndpoint) encodeKey(re *global.RowRequest, rule *global.Rule) string {
	var key string
	if rule.RedisKeyFormatter == "" {
		if rule.RedisKeyIndexListLen == 1 { // 组合ID
			key = stringutil.ToString(re.Row[rule.RedisKeyIndexList[0]])
		} else {
			for _, v := range rule.RedisKeyIndexList {
				key += stringutil.ToString(re.Row[v])
			}
		}
		if rule.RedisKeyPrefix != "" {
			key = rule.RedisKeyPrefix + key
		}
	} else {
		for column, index := range rule.RedisKeyIndexMap {
			val := stringutil.ToString(re.Row[index])
			temp := rule.RedisKeyFormatter
			temp = strings.ReplaceAll(temp, global.LeftBrace+column+global.RightBrace, val)
			key = temp
		}
	}

	return key
}

func (s *RedisEndpoint) encodeHashField(re *global.RowRequest, rule *global.Rule) string {
	var hashField string
	if rule.RedisTableHashFieldIndexListLen == 1 {
		hashField = stringutil.ToString(re.Row[rule.RedisTableHashFieldIndexList[0]])
	} else {
		for _, index := range rule.RedisTableHashFieldIndexList {
			hashField += stringutil.ToString(re.Row[index])
		}
	}
	if rule.RedisKeyPrefix != "" {
		hashField = rule.RedisKeyPrefix + hashField
	}

	return hashField
}

func (s *RedisEndpoint) StartRetryTask() {
	ticker := time.NewTicker(_retryInterval * time.Second)
	go func() {
		for {
			<-ticker.C
			if _rowCache.Size() == 0 {
				continue
			}
			if err := s.Ping(); err != nil {
				continue
			}
			logutil.Infof("重试队列有 %d条数据", _rowCache.Size())
			list, err := _rowCache.List()
			if err != nil {
				logutil.Errorf(err.Error())
				continue
			}
			for k, v := range list {
				var cached global.RowRequest
				err = msgpack.Unmarshal(v, &cached)
				if err != nil {
					logutil.Errorf(err.Error())
					_rowCache.Delete(k)
					continue
				}

				rule, _ := global.RuleIns(cached.RuleKey)
				if rule.LuaNecessary() {
					err = s.doLuaRetry(&cached, rule)
				} else {
					err = s.doLuaRetry(&cached, rule)
				}

				if err != nil {
					break
				}

				logutil.Info("数据重试成功")
				_rowCache.Delete(k)
			}
		}
	}()
}

func (s *RedisEndpoint) doLuaRetry(row *global.RowRequest, rule *global.Rule) error {
	kvm := keyValueMap(row, rule)

	responds, err := luaengine.DoRedisOps(kvm, rule)
	if err != nil {
		logutil.Warn(err.Error())
		return nil
	}

	var cmder redis.Cmdable
	if s.isCluster {
		cmder = s.cluster
	} else {
		cmder = s.client
	}

	for _, resp := range responds {
		switch rule.RedisStructure {
		case global.RedisStructureString:
			if row.Action == global.DeleteAction {
				s := cmder.Del(resp.Key)
				if s.Err() != nil {
					return s.Err()
				}
			} else {
				s := cmder.Set(resp.Key, resp.Val, 0)
				if s.Err() != nil {
					return s.Err()
				}
			}
		case global.RedisStructureHash:
			field := s.encodeHashField(row, rule)
			if row.Action == global.DeleteAction {
				s := cmder.HDel(resp.Key, field)
				if s.Err() != nil {
					return s.Err()
				}
			} else {
				s := cmder.HSet(resp.Key, field, resp.Val)
				if s.Err() != nil {
					return s.Err()
				}
			}
		case global.RedisStructureList:
			if row.Action == global.DeleteAction {
				s := cmder.LRem(rule.RedisKeyValue, 0, resp.Val)
				if s.Err() != nil {
					return s.Err()
				}
			} else {
				s := cmder.RPush(rule.RedisKeyValue, resp.Val)
				if s.Err() != nil {
					return s.Err()
				}
			}
		case global.RedisStructureSet:
			if row.Action == global.DeleteAction {
				s := cmder.SRem(rule.RedisKeyValue, resp.Val)
				if s.Err() != nil {
					return s.Err()
				}
			} else {
				s := cmder.SAdd(rule.RedisKeyValue, resp.Val)
				if s.Err() != nil {
					return s.Err()
				}
			}
		}
		logutil.Infof("%s by lua : %v", row.RuleKey, resp)
	}

	return nil
}

func (s *RedisEndpoint) doRuleRetry(row *global.RowRequest, rule *global.Rule) error {
	kvm := keyValueMap(row, rule)

	var cmder redis.Cmdable
	if s.isCluster {
		cmder = s.cluster
	} else {
		cmder = s.client
	}

	key := s.encodeKey(row, rule)
	val := encodeStringValue(rule.ValueEncoder, kvm)
	switch rule.RedisStructure {
	case global.RedisStructureString:
		if row.Action == global.DeleteAction {
			s := cmder.Del(key)
			if s.Err() != nil {
				return s.Err()
			}
		} else {
			s := cmder.Set(key, val, 0)
			if s.Err() != nil {
				return s.Err()
			}
		}
	case global.RedisStructureHash:
		field := s.encodeHashField(row, rule)
		if row.Action == global.DeleteAction {
			s := cmder.HDel(key, field)
			if s.Err() != nil {
				return s.Err()
			}
		} else {
			s := cmder.HSet(key, field, val)
			if s.Err() != nil {
				return s.Err()
			}
		}
	case global.RedisStructureList:
		if row.Action == global.DeleteAction {
			s := cmder.LRem(rule.RedisKeyValue, 0, val)
			if s.Err() != nil {
				return s.Err()
			}
		} else {
			s := cmder.RPush(rule.RedisKeyValue, val)
			if s.Err() != nil {
				return s.Err()
			}
		}
	case global.RedisStructureSet:
		if row.Action == global.DeleteAction {
			s := cmder.SRem(rule.RedisKeyValue, val)
			if s.Err() != nil {
				return s.Err()
			}
		} else {
			s := cmder.SAdd(rule.RedisKeyValue, val)
			if s.Err() != nil {
				return s.Err()
			}
		}
	}

	return nil
}

func (s *RedisEndpoint) Close() {
	if s.client != nil {
		s.client.Close()
	}
}

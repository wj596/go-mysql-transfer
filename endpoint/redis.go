package endpoint

import (
	"bytes"
	"fmt"
	"strconv"
	"sync"

	"github.com/go-redis/redis"
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"

	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/endpoint/luaengine"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/stringutils"
)

type RedisEndpoint struct {
	info          *po.EndpointInfo
	singleClient  *redis.Client
	clusterClient *redis.ClusterClient
	pipeliner     redis.Pipeliner
	retryLock     sync.Mutex
}

func newRedisEndpoint(info *po.EndpointInfo) *RedisEndpoint {
	r := &RedisEndpoint{}
	r.info = info
	return r
}

func (s *RedisEndpoint) Connect() error {
	if isCluster(s.info) {
		if s.info.GroupType == constants.RedisGroupTypeSentinel {
			client := redis.NewFailoverClient(&redis.FailoverOptions{
				MasterName:    s.info.MasterName,
				SentinelAddrs: getAddressList(s.info),
				Password:      s.info.Password,
				DB:            int(s.info.Database),
			})
			s.singleClient = client
		}

		if s.info.GroupType == constants.RedisGroupTypeCluster {
			client := redis.NewClusterClient(&redis.ClusterOptions{
				Addrs:    getAddressList(s.info),
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

	if s.singleClient != nil {
		s.pipeliner = s.singleClient.Pipeline()
	}
	if s.clusterClient != nil {
		s.pipeliner = s.clusterClient.Pipeline()
	}

	return s.Ping()
}

func (s *RedisEndpoint) Ping() error {
	var err error

	if s.singleClient != nil {
		_, err = s.singleClient.Ping().Result()
	}

	if s.clusterClient != nil {
		_, err = s.clusterClient.Ping().Result()
	}

	return err
}

func (s *RedisEndpoint) Consume(requests []*bo.RowEventRequest) error {
	var err error

	for _, request := range requests {
		rule, _ := bo.RuntimeRules[request.RuleKey]
		if rule.GetTableColumnCount() != len(request.Row) {
			log.Warnf("%s schema mismatching", request.RuleKey)
			continue
		}

		//TODO
		//metrics.UpdateActionNum(row.Action, row.RuleKey)

		if rule.IsLuaScript() {
			err = s.luaConsume(request, rule)
			bo.ReleaseRowEventRequest(request)
			if err != nil {
				break
			}
		} else {
			err = s.regulationConsume(request, rule)
			bo.ReleaseRowEventRequest(request)
			if err != nil {
				break
			}
		}
	}

	if err != nil {
		fmt.Println("-----------------")
		fmt.Println(err)
		fmt.Println("-----------------")
		return err
	}

	res, err := s.pipeliner.Exec()
	fmt.Println(err)
	for _, re := range res {
		fmt.Println(re.Err())
		fmt.Println(re.Args())
	}
	return err
}

func (s *RedisEndpoint) regulationConsume(request *bo.RowEventRequest, rt *bo.RuntimeRule) error {
	rule := rt.GetDef()
	key, err := s.encodeKey(request, rule, rt)
	if err != nil {
		return err
	}

	switch rule.GetRedisStructure() {
	case constants.RedisStructureString:
		if request.Action == canal.DeleteAction {
			s.pipeliner.Del(key)
		} else {
			value, err := rt.EncodeValue(request)
			if err != nil {
				return err
			}
			s.pipeliner.Set(key, value, 0)
		}
	case constants.RedisStructureHash:
		field, err := s.encodeHashField(request, rule, rt)
		if err != nil {
			return err
		}
		if request.Action == canal.DeleteAction {
			s.pipeliner.HDel(key, field)
		} else {
			value, err := rt.EncodeValue(request)
			if err != nil {
				return err
			}
			s.pipeliner.HSet(key, field, value)
		}
	case constants.RedisStructureList:
		value, err := rt.EncodeValue(request)
		if err != nil {
			return err
		}
		if request.Action == canal.DeleteAction {
			s.pipeliner.LRem(key, 0, value)
		} else if request.Action == canal.UpdateAction {
			coveredValue, err := rt.EncodeCoveredValue(request)
			if err != nil {
				return err
			}
			s.pipeliner.LRem(key, 0, coveredValue)
			s.pipeliner.RPush(key, value)
		} else {
			s.pipeliner.RPush(key, value)
		}
	case constants.RedisStructureSet:
		value, err := rt.EncodeValue(request)
		if err != nil {
			return err
		}
		if request.Action == canal.DeleteAction {
			s.pipeliner.SRem(key, value)
		} else if request.Action == canal.UpdateAction {
			coveredValue, err := rt.EncodeCoveredValue(request)
			if err != nil {
				return err
			}
			s.pipeliner.SRem(key, 0, coveredValue)
			s.pipeliner.SAdd(key, value)
		} else {
			s.pipeliner.SAdd(key, value)
		}
	case constants.RedisStructureSortedSet:
		value, err := rt.EncodeValue(request)
		if err != nil {
			return err
		}
		if request.Action == canal.DeleteAction {
			s.pipeliner.ZRem(key, value)
		} else if request.Action == canal.UpdateAction {
			coveredValue, err := rt.EncodeCoveredValue(request)
			if err != nil {
				return err
			}
			score, err := s.encodeScoreField(request, rule, rt)
			if err != nil {
				return err
			}
			s.pipeliner.ZRem(key, 0, coveredValue)
			val := redis.Z{Score: score, Member: value}
			s.pipeliner.ZAdd(key, val)
		} else {
			score, err := s.encodeScoreField(request, rule, rt)
			if err != nil {
				return err
			}
			val := redis.Z{Score: score, Member: value}
			s.pipeliner.ZAdd(key, val)
		}
	}

	return nil
}

func (s *RedisEndpoint) luaConsume(request *bo.RowEventRequest, rt *bo.RuntimeRule) error {
	results, err := luaengine.ExecuteRedisModule(rt.GetRawRowMap(request), rt.GetRawCoveredMap(request), request.Action, rt)
	if err != nil {
		log.Warnf("Lua脚本执行失败：%s", errors.ErrorStack(err))
		return err
	}

	for _, resp := range results {
		switch resp.Structure {
		case constants.RedisStructureString:
			if request.Action == canal.DeleteAction {
				s.pipeliner.Del(resp.Key)
			} else {
				s.pipeliner.Set(resp.Key, resp.Value, 0)
			}
		case constants.RedisStructureHash:
			if resp.Action == canal.DeleteAction {
				s.pipeliner.HDel(resp.Key, resp.Field)
			} else {
				s.pipeliner.HSet(resp.Key, resp.Field, resp.Value)
			}
		case constants.RedisStructureList:
			if resp.Action == canal.DeleteAction {
				s.pipeliner.LRem(resp.Key, 0, resp.Value)
			} else if resp.Action == canal.UpdateAction {
				s.pipeliner.LRem(resp.Key, 0, resp.CoveredValue)
				s.pipeliner.RPush(resp.Key, resp.Value)
			} else {
				s.pipeliner.RPush(resp.Key, resp.Value)
			}
		case constants.RedisStructureSet:
			if resp.Action == canal.DeleteAction {
				s.pipeliner.SRem(resp.Key, resp.Value)
			} else if resp.Action == canal.UpdateAction {
				s.pipeliner.SRem(resp.Key, 0, resp.CoveredValue)
				s.pipeliner.SAdd(resp.Key, resp.Value)
			} else {
				s.pipeliner.SAdd(resp.Key, resp.Value)
			}
		case constants.RedisStructureSortedSet:
			if resp.Action == canal.DeleteAction {
				s.pipeliner.ZRem(resp.Key, resp.Value)
			} else if resp.Action == canal.UpdateAction {
				s.pipeliner.ZRem(resp.Key, 0, resp.CoveredValue)
				val := redis.Z{Score: resp.Score, Member: resp.Value}
				s.pipeliner.ZAdd(resp.Key, val)
			} else {
				val := redis.Z{Score: resp.Score, Member: resp.Value}
				s.pipeliner.ZAdd(resp.Key, val)
			}
		}
		bo.ReleaseRedisLuaExeResult(resp)
	}

	return nil
}

func (s *RedisEndpoint) FullSync([]interface{}, *po.TransformRule) (int64, error) {
	return 0, nil
}

func (s *RedisEndpoint) encodeKey(raw *bo.RowEventRequest, rule *po.TransformRule, rt *bo.RuntimeRule) (string, error) {
	if constants.RedisStructureString != rule.GetRedisStructure() {
		return rule.RedisKeyFixValue, nil
	}

	if constants.RedisKeyBuilderColumnValue == rule.GetRedisKeyBuilder() {
		index := rt.GetTableColumnIndex(rule.GetRedisKeyColumn())
		if index < 0 {
			return "", errors.Errorf("Redis Key列[%s],不在表结构中", rule.GetRedisKeyColumn())
		}
		key := stringutils.ToString(raw.Row[index])
		if rule.GetRedisKeyPrefix() != "" {
			key = rule.GetRedisKeyPrefix() + key
		}
		return key, nil
	}

	if constants.RedisKeyBuilderExpression == rule.GetRedisKeyBuilder() {
		kv := rt.GetRawRowMap(raw)
		var tmplBytes bytes.Buffer
		err := rt.GetRedisKeyExpressionTmpl().Execute(&tmplBytes, kv)
		if err != nil {
			return "", err
		}
		return tmplBytes.String(), nil
	}

	return "", errors.New("请先设置KEY生成方式")
}

func (s *RedisEndpoint) encodeHashField(raw *bo.RowEventRequest, rule *po.TransformRule, rt *bo.RuntimeRule) (string, error) {
	index := rt.GetTableColumnIndex(rule.GetRedisHashFieldColumn())
	if index < 0 {
		return "", errors.Errorf("Redis Field列[%s],不在表结构中", rule.GetRedisKeyColumn())
	}
	field := stringutils.ToString(raw.Row[index])
	if rule.GetRedisHashFieldPrefix() != "" {
		field = rule.GetRedisHashFieldPrefix() + field
	}
	return field, nil
}

func (s *RedisEndpoint) encodeScoreField(raw *bo.RowEventRequest, rule *po.TransformRule, rt *bo.RuntimeRule) (float64, error) {
	index := rt.GetTableColumnIndex(rule.GetRedisSortedSetScoreColumn())
	if index < 0 {
		return 0, errors.Errorf("Redis Score列[%s],不在表结构中", rule.GetRedisKeyColumn())
	}
	vv := raw.Row[index]
	str := stringutils.ToString(vv)
	score, err := strconv.ParseFloat(str, 64)
	if nil != err {
		return 0, errors.Errorf("Redis Score列[%s],必须是数字类型", rule.GetRedisKeyColumn())
	}
	return score, nil
}

func (s *RedisEndpoint) Close() {
	if s.singleClient != nil {
		s.singleClient.Close()
	}
	if s.clusterClient != nil {
		s.clusterClient.Close()
	}
}

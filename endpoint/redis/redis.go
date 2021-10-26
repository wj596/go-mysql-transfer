package redis

import (
	"bytes"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/yuin/gopher-lua"

	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/endpoint/common"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/stringutils"
)

type Endpoint struct {
	info          *po.EndpointInfo
	singleClient  *redis.Client
	clusterClient *redis.ClusterClient
	pipeline      redis.Pipeliner
	retryLock     sync.Mutex
}

func NewEndpoint(info *po.EndpointInfo) *Endpoint {
	return &Endpoint{
		info: info,
	}
}

func (s *Endpoint) Connect() error {
	if common.IsCluster(s.info) {
		if s.info.GroupType == constants.RedisGroupTypeSentinel {
			client := redis.NewFailoverClient(&redis.FailoverOptions{
				MasterName:    s.info.MasterName,
				SentinelAddrs: common.GetAddressList(s.info),
				Password:      s.info.Password,
				DB:            int(s.info.Database),
			})
			s.singleClient = client
		}

		if s.info.GroupType == constants.RedisGroupTypeCluster {
			client := redis.NewClusterClient(&redis.ClusterOptions{
				Addrs:    common.GetAddressList(s.info),
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
		s.pipeline = s.singleClient.Pipeline()
	}
	if s.clusterClient != nil {
		s.pipeline = s.clusterClient.Pipeline()
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

func (s *Endpoint) byLua(request *bo.RowEventRequest, ctx *bo.RuleContext, L *lua.LState) error {
	event := L.NewTable()
	row := L.NewTable()
	common.PaddingLTable(L, row, ctx.GetRow(request))
	L.SetTable(event, common.RowKey, row)
	if canal.UpdateAction == request.Action {
		preRow := L.NewTable()
		common.PaddingLTable(L, preRow, ctx.GetPreRow(request))
		L.SetTable(event, common.PreRowKey, preRow)
	}
	L.SetTable(event, common.ActionKey, lua.LString(request.Action))

	result := L.NewTable()
	L.SetGlobal(common.GlobalVariableResult, result)

	err := L.CallByParam(lua.P{
		Fn:      L.GetGlobal(common.HandleFunctionName),
		NRet:    0,
		Protect: true,
	}, event)
	if err != nil {
		return err
	}

	result.ForEach(func(k lua.LValue, v lua.LValue) {
		kk := common.LValueToString(k)
		action := kk[0:6]
		if constants.ActionExpire == action {
			key := kk[7:len(kk)]
			val := stringutils.ToInt64Safe(common.LValueToString(v))
			expiration := time.Duration(val)
			s.pipeline.Expire(key, expiration*time.Second)
		} else {
			structure, _ := strconv.Atoi(kk[7:8])
			switch structure {
			case constants.RedisStructureString:
				key := kk[9:len(kk)]
				log.Infof("[%s] 接收端[redis]、Structure[String]、事件[%s]、KEY[%s]", ctx.GetPipelineName(), request.Action, key)
				if action == canal.DeleteAction {
					s.pipeline.Del(key)
				} else {
					value := common.LValueToInterface(v, true)
					s.pipeline.Set(key, value, 0)
				}
			case constants.RedisStructureList:
				key := kk[9:len(kk)]
				val := common.LValueToInterface(v, true)
				log.Infof("[%s] 接收端[redis]、Structure[List]、事件[%s]、KEY[%s]", ctx.GetPipelineName(), request.Action, key)
				if action == canal.DeleteAction {
					s.pipeline.LRem(key, 0, val)
				} else {
					s.pipeline.RPush(key, val)
				}
			case constants.RedisStructureSet:
				key := kk[9:len(kk)]
				val := common.LValueToInterface(v, true)
				log.Infof("[%s] 接收端[redis]、Structure[Set]、事件[%s]、KEY[%s]", ctx.GetPipelineName(), request.Action, key)
				if action == canal.DeleteAction {
					s.pipeline.SRem(key, 0, val)
				} else {
					s.pipeline.SAdd(key, val)
				}
			case constants.RedisStructureHash:
				luaKey := L.GetTable(v, lua.LString("key"))
				luaField := L.GetTable(v, lua.LString("field"))
				key := luaKey.String()
				field := common.LValueToString(luaField)
				log.Infof("[%s] 接收端[redis]、Structure[Hash]、事件[%s]、KEY[%s]、FIELD[%s]", ctx.GetPipelineName(), action, key, field)
				if action == canal.DeleteAction {
					s.pipeline.HDel(key, field)
				} else {
					luaVal := L.GetTable(v, lua.LString("val"))
					val := common.LValueToInterface(luaVal, true)
					s.pipeline.HSet(key, field, val)
				}
			case constants.RedisStructureSortedSet:
				luaKey := L.GetTable(v, lua.LString("key"))
				key := luaKey.String()
				luaVal := L.GetTable(v, lua.LString("val"))
				val := common.LValueToInterface(luaVal, true)
				log.Infof("[%s] 接收端[redis]、Structure[SortedSet]、事件[%s]、KEY[%s]、FIELD[%s]", ctx.GetPipelineName(), action, key)
				if action == canal.DeleteAction {
					s.pipeline.ZRem(key, val)
				} else {
					luaScore := common.LValueToString(L.GetTable(v, lua.LString("score")))
					score := stringutils.ToFloat64Safe(luaScore)
					z := redis.Z{Score: score, Member: val}
					s.pipeline.ZAdd(key, z)
				}
			}
		}
	})

	return nil
}

func (s *Endpoint) byRegular(request *bo.RowEventRequest, ctx *bo.RuleContext) error {
	rule := ctx.GetRule()

	key, err := s.encodeKey(request, rule, ctx)
	if err != nil {
		return err
	}

	switch rule.GetRedisStructure() {
	case constants.RedisStructureString:
		log.Infof("[%s] 接收端[redis]、Structure[String]、事件[%s]、KEY[%s]", ctx.GetPipelineName(), request.Action, key)
		if request.Action == canal.DeleteAction {
			s.pipeline.Del(key)
		} else {
			value, err := ctx.EncodeValue(request)
			if err != nil {
				return err
			}
			s.pipeline.Set(key, value, 0)
		}
	case constants.RedisStructureHash:
		field, err := s.encodeHashField(request, rule, ctx)
		log.Infof("[%s] 接收端[redis]、Structure[Hash]、事件[%s]、KEY[%s]、FIELD[%s]", ctx.GetPipelineName(), request.Action, key, field)
		if err != nil {
			return err
		}
		if request.Action == canal.DeleteAction {
			s.pipeline.HDel(key, field)
		} else {
			value, err := ctx.EncodeValue(request)
			if err != nil {
				return err
			}
			s.pipeline.HSet(key, field, value)
		}
	case constants.RedisStructureList:
		log.Infof("[%s] 接收端[redis]、Structure[List]、事件[%s]、KEY[%s]", ctx.GetPipelineName(), request.Action, key)
		value, err := ctx.EncodeValue(request)
		if err != nil {
			return err
		}
		if request.Action == canal.DeleteAction {
			s.pipeline.LRem(key, 0, value)
		} else if request.Action == canal.UpdateAction {
			preValue, err := ctx.EncodePreValue(request)
			if err != nil {
				return err
			}
			s.pipeline.LRem(key, 0, preValue)
			s.pipeline.RPush(key, value)
		} else {
			s.pipeline.RPush(key, value)
		}
	case constants.RedisStructureSet:
		log.Infof("[%s] 接收端[redis]、Structure[Set]、事件[%s]、KEY[%s]", ctx.GetPipelineName(), request.Action, key)
		value, err := ctx.EncodeValue(request)
		if err != nil {
			return err
		}
		if request.Action == canal.DeleteAction {
			s.pipeline.SRem(key, value)
		} else if request.Action == canal.UpdateAction {
			preValue, err := ctx.EncodePreValue(request)
			if err != nil {
				return err
			}
			s.pipeline.SRem(key, 0, preValue) //移除集中之前的数据
			s.pipeline.SAdd(key, value)
		} else {
			s.pipeline.SAdd(key, value)
		}
	case constants.RedisStructureSortedSet:
		log.Infof("[%s] 接收端[redis]、Structure[SortedSet]、事件[%s]、KEY[%s]", ctx.GetPipelineName(), request.Action, key)
		value, err := ctx.EncodeValue(request)
		if err != nil {
			return err
		}
		if request.Action == canal.DeleteAction {
			s.pipeline.ZRem(key, value)
		} else if request.Action == canal.UpdateAction {
			preValue, err := ctx.EncodePreValue(request)
			if err != nil {
				return err
			}

			var score float64
			score, err = s.encodeScoreField(request, rule, ctx)
			if err != nil {
				return err
			}
			s.pipeline.ZRem(key, 0, preValue) //移除有序集中之前的数据
			val := redis.Z{Score: score, Member: value}
			s.pipeline.ZAdd(key, val)
		} else {
			score, err := s.encodeScoreField(request, rule, ctx)
			if err != nil {
				return err
			}
			val := redis.Z{Score: score, Member: value}
			s.pipeline.ZAdd(key, val)
		}
	}

	return nil
}

func (s *Endpoint) encodeKey(raw *bo.RowEventRequest, rule *po.TransformRule, ctx *bo.RuleContext) (string, error) {
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

func (s *Endpoint) encodeHashField(raw *bo.RowEventRequest, rule *po.TransformRule, ctx *bo.RuleContext) (string, error) {
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

func (s *Endpoint) encodeScoreField(raw *bo.RowEventRequest, rule *po.TransformRule, ctx *bo.RuleContext) (float64, error) {
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

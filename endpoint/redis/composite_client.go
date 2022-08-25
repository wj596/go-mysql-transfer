package redis

import (
	"strings"
	"time"

	"github.com/go-redis/redis"
	"github.com/juju/errors"

	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/util/stringutils"
)

type CompositeClient struct {
	info          *po.EndpointInfo
	key           string
	isCluster     bool
	singleClient  *redis.Client
	clusterClient *redis.ClusterClient
}

func NewCompositeClient(info *po.EndpointInfo) *CompositeClient {
	return &CompositeClient{
		key:  stringutils.ToString(info.Id),
		info: info,
	}
}

func (s *CompositeClient) Connect() error {
	addresses := strings.Split(s.info.GetAddresses(), stringutils.Comma)
	if len(addresses) > 1 {
		if s.info.GroupType == constants.RedisGroupTypeSentinel {
			client := redis.NewFailoverClient(&redis.FailoverOptions{
				MasterName:    s.info.MasterName,
				SentinelAddrs: addresses,
				Password:      s.info.Password,
				DB:            int(s.info.Database),
			})
			s.singleClient = client
		}

		if s.info.GroupType == constants.RedisGroupTypeCluster {
			client := redis.NewClusterClient(&redis.ClusterOptions{
				Addrs:    addresses,
				Password: s.info.Password,
			})
			s.clusterClient = client
		}

		s.isCluster = true
	} else {
		client := redis.NewClient(&redis.Options{
			Addr:     s.info.GetAddresses(),
			Password: s.info.Password,
			DB:       int(s.info.Database),
		})
		s.singleClient = client
		s.isCluster = false
	}

	if s.singleClient == nil && s.clusterClient == nil {
		return errors.Errorf("Redis客户端创建失败")
	}

	return nil
}

func (s *CompositeClient) Ping() error {
	var err error
	if s.isCluster {
		_, err = s.clusterClient.Ping().Result()
	} else {
		_, err = s.singleClient.Ping().Result()
	}
	return err
}

func (s *CompositeClient) Close() {
	if s.isCluster {
		s.clusterClient.Close()
	} else {
		s.singleClient.Close()
	}
}

func (s *CompositeClient) createPipeline() redis.Pipeliner {
	var pipe redis.Pipeliner
	if s.isCluster {
		pipe = s.clusterClient.Pipeline()
	} else {
		pipe = s.singleClient.Pipeline()
	}
	return pipe
}

func (s *CompositeClient) Set(key string, value interface{}) (string, error) {
	var result string
	var err error

	if s.isCluster {
		result, err = s.clusterClient.Set(key, value, -1).Result()
	} else {
		result, err = s.singleClient.Set(key, value, -1).Result()
	}
	return result, err
}

func (s *CompositeClient) Del(key string) (int64, error) {
	var result int64
	var err error

	if s.isCluster {
		result, err = s.clusterClient.Del(key).Result()
	} else {
		result, err = s.singleClient.Del(key).Result()
	}
	return result, err
}

func (s *CompositeClient) Get(key string) (string, error) {
	var result string
	var err error

	if s.isCluster {
		result, err = s.clusterClient.Get(key).Result()
	} else {
		result, err = s.singleClient.Get(key).Result()
	}
	return result, err
}

func (s *CompositeClient) Incr(key string) (int64, error) {
	var result int64
	var err error

	if s.isCluster {
		result, err = s.clusterClient.Incr(key).Result()
	} else {
		result, err = s.singleClient.Incr(key).Result()
	}
	return result, err
}

func (s *CompositeClient) Decr(key string) (int64, error) {
	var result int64
	var err error

	if s.isCluster {
		result, err = s.clusterClient.Decr(key).Result()
	} else {
		result, err = s.singleClient.Decr(key).Result()
	}
	return result, err
}

func (s *CompositeClient) Append(key, val string) (int64, error) {
	var result int64
	var err error

	if s.isCluster {
		result, err = s.clusterClient.Append(key, val).Result()
	} else {
		result, err = s.singleClient.Append(key, val).Result()
	}
	return result, err
}

func (s *CompositeClient) HGet(key, field string) (string, error) {
	var result string
	var err error

	if s.isCluster {
		result, err = s.clusterClient.HGet(key, field).Result()
	} else {
		result, err = s.singleClient.HGet(key, field).Result()
	}
	return result, err
}

func (s *CompositeClient) HExists(key, field string) (bool, error) {
	var result bool
	var err error

	if s.isCluster {
		result, err = s.clusterClient.HExists(key, field).Result()
	} else {
		result, err = s.singleClient.HExists(key, field).Result()
	}
	return result, err
}

func (s *CompositeClient) HSet(key, field string, value interface{}) (bool, error) {
	var result bool
	var err error

	if s.isCluster {
		result, err = s.clusterClient.HSet(key, field, value).Result()
	} else {
		result, err = s.singleClient.HSet(key, field, value).Result()
	}
	return result, err
}

func (s *CompositeClient) HDel(key, field string) (int64, error) {
	var result int64
	var err error

	if s.isCluster {
		result, err = s.clusterClient.HDel(key, field).Result()
	} else {
		result, err = s.singleClient.HDel(key, field).Result()
	}
	return result, err
}

func (s *CompositeClient) RPush(key string, value interface{}) (int64, error) {
	var result int64
	var err error

	if s.isCluster {
		result, err = s.clusterClient.RPush(key, value).Result()
	} else {
		result, err = s.singleClient.RPush(key, value).Result()
	}
	return result, err
}

func (s *CompositeClient) LRem(key string, value interface{}) (int64, error) {
	var result int64
	var err error

	if s.isCluster {
		result, err = s.clusterClient.LRem(key, 0, value).Result()
	} else {
		result, err = s.singleClient.LRem(key, 0, value).Result()
	}
	return result, err
}

func (s *CompositeClient) SAdd(key string, value interface{}) (int64, error) {
	var result int64
	var err error

	if s.isCluster {
		result, err = s.clusterClient.SAdd(key, 0, value).Result()
	} else {
		result, err = s.singleClient.SAdd(key, 0, value).Result()
	}
	return result, err
}

func (s *CompositeClient) SRem(key string, value interface{}) (int64, error) {
	var result int64
	var err error

	if s.isCluster {
		result, err = s.clusterClient.SRem(key, value).Result()
	} else {
		result, err = s.singleClient.SRem(key, value).Result()
	}
	return result, err
}

func (s *CompositeClient) ZAdd(key string, score float64, value interface{}) (int64, error) {
	var result int64
	var err error

	val := redis.Z{Score: score, Member: value}
	if s.isCluster {
		result, err = s.clusterClient.ZAdd(key, val).Result()
	} else {
		result, err = s.singleClient.ZAdd(key, val).Result()
	}
	return result, err
}

func (s *CompositeClient) ZRem(key string, value interface{}) (int64, error) {
	var result int64
	var err error
	if s.isCluster {
		result, err = s.clusterClient.ZRem(key, value).Result()
	} else {
		result, err = s.singleClient.ZRem(key, value).Result()
	}
	return result, err
}

func (s *CompositeClient) Expire(key string, expiration time.Duration) (bool, error) {
	var result bool
	var err error
	if s.isCluster {
		result, err = s.clusterClient.Expire(key, expiration).Result()
	} else {
		result, err = s.singleClient.Expire(key, expiration).Result()
	}
	return result, err
}
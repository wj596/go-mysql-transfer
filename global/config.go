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
package global

import (
	"fmt"
	"github.com/juju/errors"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"path/filepath"
	"strings"

	"go-mysql-transfer/util/fileutil"
	"go-mysql-transfer/util/logutil"
	"go-mysql-transfer/util/netutil"
)

const (
	_targetRedis         = "REDIS"
	_targetMongodb       = "MONGODB"
	_targetRocketmq      = "ROCKETMQ"
	_targetRabbitmq      = "RABBITMQ"
	_targetKafka         = "KAFKA"
	_targetElasticsearch = "ELASTICSEARCH"

	RedisGroupTypeSentinel = "sentinel"
	RedisGroupTypeCluster  = "cluster"

	_dataDir = "store"

	_zeRootDir = "/transfer" // ZooKeeper and Etcd root

	_flushBulkInterval  = 200
	_flushBulkSize      = 128
	_redisFlushBulkSize = 1024

	DefaultDateFormatter     = "2006-01-02"
	DefaultDatetimeFormatter = "2006-01-02 15:04:05"
)

var (
	_clusterFlag bool
	_config      *Config
)

type Config struct {
	Target string `yaml:"target"` // 目标类型，支持redis、mongodb

	Addr     string `yaml:"addr"`
	User     string `yaml:"user"`
	Password string `yaml:"pass"`
	Charset  string `yaml:"charset"`

	SlaveID uint32 `yaml:"slave_id"`
	Flavor  string `yaml:"flavor"`
	DataDir string `yaml:"data_dir"`

	DumpExec       string `yaml:"mysqldump"`
	SkipMasterData bool   `yaml:"skip_master_data"`

	BulkSize int `yaml:"bulk_size"`

	FlushBulkInterval int `yaml:"flush_bulk_interval"`

	SkipNoPkTable bool `yaml:"skip_no_pk_table"`

	RuleConfigs []*Rule `yaml:"rule"`

	LoggerConfig *logutil.LoggerConfig `yaml:"logger"` // 日志配置

	EnableExporter bool `yaml:"enable_exporter"` // 启用prometheus exporter，默认false
	ExporterPort   int  `yaml:"exporter_addr"`   // prometheus exporter端口

	Cluster *Cluster `yaml:"cluster"` // 集群配置

	// ------------------- REDIS -----------------
	RedisAddr       string `yaml:"redis_addrs"`       //redis地址
	RedisGroupType  string `yaml:"redis_group_type"`  //集群类型 sentinel或者cluster
	RedisMasterName string `yaml:"redis_master_name"` //Master节点名称
	RedisPass       string `yaml:"redis_pass"`        //redis密码
	RedisDatabase   int    `yaml:"redis_database"`    //redis数据库

	// ------------------- ROCKETMQ -----------------
	RocketmqNameServers  string `yaml:"rocketmq_name_servers"`  //rocketmq命名服务地址，多个用逗号分隔
	RocketmqGroupName    string `yaml:"rocketmq_group_name"`    //rocketmq group name,默认为空
	RocketmqInstanceName string `yaml:"rocketmq_instance_name"` //rocketmq instance name,默认为空
	RocketmqAccessKey    string `yaml:"rocketmq_access_key"`    //访问控制 accessKey,默认为空
	RocketmqSecretKey    string `yaml:"rocketmq_secret_key"`    //访问控制 secretKey,默认为空

	// ------------------- MONGODB -----------------
	MongodbAddr     string `yaml:"mongodb_addrs"`    //mongodb地址，多个用逗号分隔
	MongodbUsername string `yaml:"mongodb_username"` //mongodb用户名，默认为空
	MongodbPassword string `yaml:"mongodb_password"` //mongodb密码，默认为空

	// ------------------- RABBITMQ -----------------
	RabbitmqAddr string `yaml:"rabbitmq_addr"` //连接字符串,如: amqp://guest:guest@localhost:5672/

	// ------------------- KAFKA -----------------
	KafkaAddr         string `yaml:"kafka_addrs"`         //kafka连接地址，多个用逗号分隔
	KafkaSASLUser     string `yaml:"kafka_sasl_user"`     //kafka SASL_PLAINTEXT认证模式 用户名
	KafkaSASLPassword string `yaml:"kafka_sasl_password"` //kafka SASL_PLAINTEXT认证模式 密码

	// ------------------- ES -----------------
	ElsAddr     string `yaml:"es_addrs"`    //Elasticsearch连接地址，多个用逗号分隔
	ElsUser     string `yaml:"es_user"`     //Elasticsearch用户名
	ElsPassword string `yaml:"es_password"` //Elasticsearch密码
	ElsVersion  int    `yaml:"es_version"`  //Elasticsearch版本，支持6和7、默认为7
}

type Cluster struct {
	Name             string `yaml:"name"`
	ZkAddrs          string `yaml:"zk_addrs"`
	ZkAuthentication string `yaml:"zk_authentication"`
	EtcdAddrs        string `yaml:"etcd_addrs"`
	EtcdUser         string `yaml:"etcd_user"`
	EtcdPassword     string `yaml:"etcd_password"`
	CurrentNode      string
}

func NewConfigWithFile(name string) (*Config, error) {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var c Config

	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, errors.Trace(err)
	}

	if err := checkConfig(&c); err != nil {
		return nil, errors.Trace(err)
	}

	if err := checkClusterConfig(&c); err != nil {
		return nil, errors.Trace(err)
	}

	switch strings.ToUpper(c.Target) {
	case _targetRedis:
		if err := checkRedisConfig(&c); err != nil {
			return nil, errors.Trace(err)
		}
	case _targetRocketmq:
		if err := checkRocketmqConfig(&c); err != nil {
			return nil, errors.Trace(err)
		}
	case _targetMongodb:
		if err := checkMongodbConfig(&c); err != nil {
			return nil, errors.Trace(err)
		}
	case _targetRabbitmq:
		if err := checkRabbitmqConfig(&c); err != nil {
			return nil, errors.Trace(err)
		}
	case _targetKafka:
		if err := checkKafkaConfig(&c); err != nil {
			return nil, errors.Trace(err)
		}
	case _targetElasticsearch:
		if err := checkElsConfig(&c); err != nil {
			return nil, errors.Trace(err)
		}
	default:
		return nil, errors.Errorf("unsupported target: %s", c.Target)
	}

	_config = &c

	return &c, nil
}

func checkConfig(c *Config) error {
	if c.Target == "" {
		return errors.Errorf("empty target not allowed")
	}

	if c.Addr == "" {
		return errors.Errorf("empty addr not allowed")
	}

	if c.User == "" {
		return errors.Errorf("empty user not allowed")
	}

	if c.Password == "" {
		return errors.Errorf("empty pass not allowed")
	}

	if c.Charset == "" {
		return errors.Errorf("empty charset not allowed")
	}

	if c.SlaveID == 0 {
		return errors.Errorf("empty slave_id not allowed")
	}

	if c.Flavor == "" {
		c.Flavor = "mysql"
	}

	if c.FlushBulkInterval == 0 {
		c.FlushBulkInterval = _flushBulkInterval
	}

	if c.BulkSize == 0 {
		c.BulkSize = _flushBulkSize
		if c.IsRedis() {
			c.BulkSize = _redisFlushBulkSize
		}
	}

	if c.DataDir == "" {
		c.DataDir = filepath.Join(fileutil.GetCurrentDirectory(), _dataDir)
	}

	if err := fileutil.MkdirIfNecessary(c.DataDir); err != nil {
		return err
	}

	if c.LoggerConfig == nil {
		c.LoggerConfig = &logutil.LoggerConfig{
			Store: filepath.Join(c.DataDir, "log"),
		}
	}
	if c.LoggerConfig.Store == "" {
		c.LoggerConfig.Store = filepath.Join(c.DataDir, "log")
	}

	if err := fileutil.MkdirIfNecessary(c.LoggerConfig.Store); err != nil {
		return err
	}

	if c.ExporterPort == 0 {
		c.ExporterPort = 9595
	}

	if c.RuleConfigs == nil {
		return errors.Errorf("empty rules not allowed")
	}

	return nil
}

func Cfg() *Config {
	return _config
}

func checkClusterConfig(c *Config) error {
	if c.Cluster == nil {
		_clusterFlag = false
		return nil
	}

	if c.Cluster.ZkAddrs == "" && c.Cluster.EtcdAddrs == "" {
		_clusterFlag = false
		return nil
	}

	if c.Cluster.Name == "" {
		return errors.Errorf("empty name not allowed in cluster")
	}

	ips, err := netutil.GetIpList()
	if err != nil {
		return err
	}
	c.Cluster.CurrentNode = fmt.Sprintf("%v", ips)

	if c.IsZk() {
		logutil.BothInfof("cluster by Zookeeper")
	}
	if c.IsEtcd() {
		logutil.BothInfof("cluster by Etcd")
	}

	return nil
}

func checkRedisConfig(c *Config) error {
	if len(c.RedisAddr) == 0 {
		return errors.Errorf("empty redis_addrs not allowed")
	}

	addrList := strings.Split(c.RedisAddr, ",")
	if len(addrList) > 1 {
		if c.RedisGroupType == "" {
			return errors.Errorf("empty group_type not allowed")
		}
		if c.RedisGroupType == RedisGroupTypeSentinel && c.RedisMasterName == "" {
			return errors.Errorf("empty master_name not allowed")
		}
	}

	return nil
}

func checkRocketmqConfig(c *Config) error {
	if len(c.RocketmqNameServers) == 0 {
		return errors.Errorf("empty rocketmq_name_servers not allowed")
	}

	return nil
}

func checkMongodbConfig(c *Config) error {
	if len(c.MongodbAddr) == 0 {
		return errors.Errorf("empty mongodb_addrs not allowed")
	}

	return nil
}

func checkRabbitmqConfig(c *Config) error {
	if len(c.RabbitmqAddr) == 0 {
		return errors.Errorf("empty rabbitmq_addr not allowed")
	}

	return nil
}

func checkKafkaConfig(c *Config) error {
	if len(c.KafkaAddr) == 0 {
		return errors.Errorf("empty kafka_addrs not allowed")
	}

	return nil
}

func checkElsConfig(c *Config) error {
	if len(c.ElsAddr) == 0 {
		return errors.Errorf("empty es_addrs not allowed")
	}

	if !strings.HasPrefix(c.ElsAddr, "http") {
		c.ElsAddr = "http://" + c.ElsAddr
	}

	if c.ElsVersion == 0 {
		c.ElsVersion = 7
	}

	if !(c.ElsVersion == 6 || c.ElsVersion == 7) {
		return errors.Errorf("elasticsearch version must 6 or 7")
	}

	return nil
}

func checkLuaConfig(c *Config) error {
	return nil
}

func (c *Config) IsCluster() bool {
	if !c.IsZk() && !c.IsEtcd() {
		return false
	}

	return true
}

func (c *Config) NotCluster() bool {
	if c.IsZk() || c.IsEtcd() {
		return false
	}

	return true
}

func (c *Config) IsZk() bool {
	if c.Cluster == nil {
		return false
	}
	if c.Cluster.ZkAddrs == "" {
		return false
	}

	return true
}

func (c *Config) IsEtcd() bool {
	if c.Cluster == nil {
		return false
	}
	if c.Cluster.EtcdAddrs == "" {
		return false
	}

	return true
}

func (c *Config) IsRedis() bool {
	return strings.ToUpper(c.Target) == _targetRedis
}

func (c *Config) IsRocketmq() bool {
	return strings.ToUpper(c.Target) == _targetRocketmq
}

func (c *Config) IsMongodb() bool {
	return strings.ToUpper(c.Target) == _targetMongodb
}

func (c *Config) IsRabbitmq() bool {
	return strings.ToUpper(c.Target) == _targetRabbitmq
}

func (c *Config) IsKafka() bool {
	return strings.ToUpper(c.Target) == _targetKafka
}

func (c *Config) IsEls() bool {
	return strings.ToUpper(c.Target) == _targetElasticsearch
}

func (c *Config) IsExporterEnable() bool {
	return c.EnableExporter
}

func (c *Config) Destination() string {
	var des string
	switch strings.ToUpper(c.Target) {
	case _targetRedis:
		des += "redis("
		des += c.RedisAddr
		des += ")"
	case _targetRocketmq:
		des += "rocketmq("
		des += c.RocketmqNameServers
		des += ")"
	case _targetMongodb:
		des += "mongodb("
		des += c.MongodbAddr
		des += ")"
	case _targetRabbitmq:
		des += "rabbitmq("
		des += c.RabbitmqAddr
		des += ")"
	case _targetKafka:
		des += "kafka("
		des += c.KafkaAddr
		des += ")"
	case _targetElasticsearch:
		des += "elasticsearch("
		des += c.ElsAddr
		des += ")"
	}
	return des
}

func (c *Config) ZeRootDir() string {
	return _zeRootDir
}

func (c *Config) ZeClusterDir() string {
	return _zeRootDir + "/" + c.Cluster.Name
}

func (c *Config) ZePositionDir() string {
	return _zeRootDir + "/" + c.Cluster.Name + "/position"
}

func (c *Config) ZeElectionDir() string {
	return _zeRootDir + "/" + c.Cluster.Name + "/election"
}

func (c *Config) ZeElectedDir() string {
	return _zeRootDir + "/" + c.Cluster.Name + "/elected"
}

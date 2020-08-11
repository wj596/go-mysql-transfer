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
	_targetRedis    = "REDIS"
	_targetMongoDB  = "MONGODB"
	_targetRocketmq = "ROCKETMQ"

	RedisGroupTypeSentinel = "sentinel"
	RedisGroupTypeCluster  = "cluster"

	_dataDir = "store"

	_zeRootDir = "/transfer" // ZooKeeper and Etcd root

	InsertAction = 1
	DeleteAction = 2
	UpdateAction = 3

	_flushBulkInterval  = 200
	_flushBulkSize      = 128
	_redisFlushBulkSize = 1024
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
	Flavor   string `yaml:"flavor"`
	DataDir  string `yaml:"data_dir"`

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
	case _targetMongoDB:
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

func (c *Config) IsMongo() bool {
	return strings.ToUpper(c.Target) == _targetMongoDB
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
	case _targetMongoDB:

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

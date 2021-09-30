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

package config

import (
	"io/ioutil"
	"log"
	"path/filepath"
	"runtime"

	"github.com/juju/errors"
	"gopkg.in/yaml.v2"

	"go-mysql-transfer/util/fileutils"
	"go-mysql-transfer/util/sysutils"
)

const (
	_configUninitializedTip = "Config未初始化"
	_clusterName            = "transfer"
	_dataDir                = "store"
	_prometheusExporterPort = 9595
	_webPort                = 8060
	_rpcPort                = 7060
)

var _instance *AppConfig

// AppConfig 应用配置
type AppConfig struct {
	DataDir           string `yaml:"data_dir"`
	Maxprocs          int    `yaml:"maxprocs"` // 最大协程数，默认CPU核心数*2
	BulkSize          int64  `yaml:"bulk_size"`
	FlushBulkInterval int    `yaml:"flush_bulk_interval"`

	EnablePrometheusExporter bool `yaml:"enable_prometheus_exporter"` // 启用prometheus exporter，默认false
	PrometheusExporterPort   int  `yaml:"prometheus_exporter_addr"`   // prometheus exporter端口
	WebPort                  int  `yaml:"web_port"`                   // web管理界面端口,默认8060
	RpcPort                  int  `yaml:"rpc_port"`                   // RPC端口，用于集群节点间通信，默认7060

	LoggerConf      *LoggerConfig     `yaml:"logger"`    // 日志配置
	ClusterConf     *ClusterConfig    `yaml:"cluster"`   // 集群配置
	ConsumerConfigs []*ConsumerConfig `yaml:"consumers"` // 用户配置
}

func Initialize(fileName string) error {
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		return errors.Trace(err)
	}

	var c AppConfig
	if err := yaml.Unmarshal(data, &c); err != nil {
		return errors.Trace(err)
	}

	if err := checkConfig(&c); err != nil {
		return errors.Trace(err)
	}

	_instance = &c

	return nil
}

func checkConfig(c *AppConfig) error {
	if c.DataDir == "" {
		c.DataDir = filepath.Join(sysutils.CurrentDirectory(), _dataDir)
	}
	if err := fileutils.MkdirIfNecessary(c.DataDir); err != nil {
		return err
	}

	if c.PrometheusExporterPort == 0 {
		c.PrometheusExporterPort = _prometheusExporterPort
	}
	if c.WebPort == 0 {
		c.WebPort = _webPort
	}
	if c.RpcPort == 0 {
		c.RpcPort = _rpcPort
	}
	if c.Maxprocs <= 0 {
		c.Maxprocs = runtime.NumCPU() * 2
	}

	// Logger
	if err := checkLoggerConfig(c); err != nil {
		return errors.Trace(err)
	}

	// Cluster
	if err := checkClusterConfig(c); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func checkLoggerConfig(c *AppConfig) error {
	if c.LoggerConf == nil {
		c.LoggerConf = &LoggerConfig{
			Store: filepath.Join(c.DataDir, "log"),
		}
	}
	if c.LoggerConf.Store == "" {
		c.LoggerConf.Store = filepath.Join(c.DataDir, "log")
	}
	if err := fileutils.MkdirIfNecessary(c.LoggerConf.Store); err != nil {
		return err
	}

	return nil
}

func checkClusterConfig(c *AppConfig) error {
	if c.GetClusterConfig() == nil {
		return nil
	}
	if c.GetClusterConfig().GetZkAddrs() == "" && c.GetClusterConfig().GetEtcdAddrs() == "" {
		return nil
	}

	if c.GetClusterConfig().GetName() == "" {
		c.GetClusterConfig().Name = _clusterName
	}

	return nil
}

func GetIns() *AppConfig {
	if _instance == nil {
		log.Fatal(_configUninitializedTip)
	}
	return _instance
}

func (c *AppConfig) GetDataDir() string {
	return c.DataDir
}

func (c *AppConfig) GetMaxprocs() int {
	return c.Maxprocs
}

func (c *AppConfig) GetBulkSize() int64 {
	return c.BulkSize
}

func (c *AppConfig) GetFlushBulkInterval() int {
	return c.FlushBulkInterval
}

func (c *AppConfig) IsEnablePrometheusExporter() bool {
	return c.EnablePrometheusExporter
}

func (c *AppConfig) GetPrometheusExporterPort() int {
	return c.PrometheusExporterPort
}

func (c *AppConfig) GetWebPort() int {
	return c.WebPort
}

func (c *AppConfig) GetRpcPort() int {
	return c.WebPort
}

func (c *AppConfig) GetClusterConfig() *ClusterConfig {
	return c.ClusterConf
}

func (c *AppConfig) GetLoggerConfig() *LoggerConfig {
	return c.LoggerConf
}

func (c *AppConfig) GetConsumerConfigs() []*ConsumerConfig {
	return c.ConsumerConfigs
}

func (c *AppConfig) IsCluster() bool {
	if c.ClusterConf == nil {
		return false
	}
	return true
}

func (c *AppConfig) IsZkUsed() bool {
	if c.ClusterConf == nil {
		return false
	}
	if c.ClusterConf.ZkAddrs == "" {
		return false
	}
	return true
}

func (c *AppConfig) IsEtcdUsed() bool {
	if c.ClusterConf == nil {
		return false
	}
	if c.ClusterConf.EtcdAddrs == "" {
		return false
	}
	return true
}

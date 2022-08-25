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

package config

import (
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"

	"github.com/juju/errors"
	"gopkg.in/yaml.v2"

	"go-mysql-transfer/util/fileutils"
	"go-mysql-transfer/util/netutils"
	"go-mysql-transfer/util/sysutils"
)

const (
	_configUninitializedTip = "Config未初始化"
	_clusterName            = "transfer"
	_dataDir                = "store"
	_prometheusExporterPort = 9595
	_webPort                = 8060
)

var _instance *AppConfig

// AppConfig 应用配置
type AppConfig struct {
	DataDir   string `yaml:"data_dir"`
	WebPort   int    `yaml:"web_port"`   // Web端口,默认8060
	SecretKey string `yaml:"secret_key"` // 签名秘钥

	LoggerConf        *LoggerConfig  `yaml:"logger"`              // 日志配置
	UserConfigs       []*UserConfig  `yaml:"users"`               // 用户配置
	SmtpConf          *SmtpConfig    `yaml:"smtp"`                // SMTP协议配置
	ClusterConf       *ClusterConfig `yaml:"cluster"`             // 集群配置
	RuntimeReportCron string         `yaml:"runtime_report_cron"` // 运行时报告CRON表达式，默认每天18点: 0 0 18 * * ?
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
		return err
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

	if c.WebPort == 0 {
		c.WebPort = _webPort
	}

	if c.SecretKey == "" {
		return errors.New("配置文件错误：配置项'secret_key' 不能为空")
	}

	// Logger
	if err := checkLoggerConfig(c); err != nil {
		return errors.Trace(err)
	}

	// SMTP
	if err := checkSmtpConfig(c); err != nil {
		return err
	}

	// Cluster
	if err := checkClusterConfig(c); err != nil {
		return errors.Trace(err)
	}

	if "" == c.RuntimeReportCron {
		c.RuntimeReportCron = "0 0 18 * * ?"
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
	if c.ClusterConf == nil {
		return nil
	}

	if c.ClusterConf.BindIp != "" && !netutils.CheckIp(c.ClusterConf.BindIp) {
		return errors.New("配置文件错误：cluster配置项'node_ip'可以为空, 如果不为空必须是一个IP地址，")
	}

	if c.ClusterConf.BindIp == "" {
		ips, err := netutils.GetIpList()
		if err != nil {
			return err
		}
		if len(ips) > 1 {
			return errors.New(fmt.Sprintf(
				"检测到机器上存在多个IP地址：%v，无法确定向其他集群节点暴露那个IP。请在配置文件'cluster'->'node_ip'配置项中指定", ips))
		}
		c.ClusterConf.BindIp = ips[0]
	}

	return nil
}

func checkSmtpConfig(c *AppConfig) error {
	if c.SmtpConf != nil {
		if c.SmtpConf.Host == "" {
			return errors.New("配置项 'smtp'->'host'不能为空")
		}
		if c.SmtpConf.Port <= 0 {
			return errors.New("请正确配置 'smtp'->'port'的值")
		}
		if c.SmtpConf.User == "" {
			return errors.New("配置项 'smtp'->'user'不能为空")
		}
		if c.SmtpConf.Password == "" {
			return errors.New("配置项 'smtp'->'user'不能为空")
		}
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

func (c *AppConfig) GetWebPort() int {
	return c.WebPort
}

func (c *AppConfig) GetSecretKey() string {
	return c.SecretKey
}

func (c *AppConfig) GetClusterConfig() *ClusterConfig {
	return c.ClusterConf
}

func (c *AppConfig) GetLoggerConfig() *LoggerConfig {
	return c.LoggerConf
}

func (c *AppConfig) GetUserConfigs() []*UserConfig {
	return c.UserConfigs
}

func (c *AppConfig) GetSmtpConfig() *SmtpConfig {
	return c.SmtpConf
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

func (c *AppConfig) IsSmtpUsed() bool {
	if c.SmtpConf == nil {
		return false
	}
	return true
}

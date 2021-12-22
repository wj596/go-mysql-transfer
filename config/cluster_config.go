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

// ClusterConfig 集群配置
type ClusterConfig struct {
	BindIp           string `yaml:"bind_ip"` //绑定IP
	ZkAddrs          string `yaml:"zk_addrs"`
	ZkAuthentication string `yaml:"zk_authentication"`
	EtcdAddrs        string `yaml:"etcd_addrs"`
	EtcdUser         string `yaml:"etcd_user"`
	EtcdPassword     string `yaml:"etcd_password"`
}

func (c *ClusterConfig) GetBindIp() string {
	return c.BindIp
}

func (c *ClusterConfig) GetZkAddrs() string {
	return c.ZkAddrs
}

func (c *ClusterConfig) GetZkAuthentication() string {
	return c.ZkAuthentication
}

func (c *ClusterConfig) GetEtcdAddrs() string {
	return c.EtcdAddrs
}

func (c *ClusterConfig) GetEtcdUser() string {
	return c.EtcdUser
}

func (c *ClusterConfig) GetEtcdPassword() string {
	return c.EtcdPassword
}

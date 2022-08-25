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

package zookeeper

import (
	"strings"
	"time"

	"github.com/go-zookeeper/zk"

	"go-mysql-transfer/config"
	"go-mysql-transfer/dao/path"
	"go-mysql-transfer/util/logagent"
	"go-mysql-transfer/util/zkutils"
)

var (
	_connection       *zk.Conn
	_connectionSignal <-chan zk.Event
	_addresses        []string
)

func Initialize(config *config.AppConfig) error {
	option := zk.WithLogger(logagent.NewZkLoggerAgent())
	addresses := strings.Split(config.GetClusterConfig().GetZkAddrs(), ",")
	conn, signal, err := zk.Connect(addresses, time.Second, option) //*10)
	if err != nil {
		return err
	}

	if config.GetClusterConfig().GetZkAuthentication() != "" {
		err = conn.AddAuth("digest", []byte(config.GetClusterConfig().GetZkAuthentication()))
		if err != nil {
			return err
		}
	}

	// 初始化Root节点
	if err = zkutils.CreateNodeIfNecessary(path.GetRoot(), conn); err != nil {
		return err
	}
	// 初始化State节点

	if err = zkutils.CreateNodeIfNecessary(path.GetStateRoot(), conn); err != nil {
		return err
	}
	// 初始化Position 根节点
	if err = zkutils.CreateNodeIfNecessary(path.GetPositionRoot(), conn); err != nil {
		return err
	}
	// 初始化Machine 根节点
	if err = zkutils.CreateNodeIfNecessary(path.GetMachineRoot(), conn); err != nil {
		return err
	}
	// 初始化Metadata 根节点
	if err = zkutils.CreateNodeIfNecessary(path.GetMetadataRoot(), conn); err != nil {
		return err
	}
	// 初始化Metadata Source根节点
	if err = zkutils.CreateNodeIfNecessary(path.GetSourceMetadataRoot(), conn); err != nil {
		return err
	}
	// 初始化Metadata Endpoint根节点
	if err = zkutils.CreateNodeIfNecessary(path.GetEndpointMetadataRoot(), conn); err != nil {
		return err
	}
	// 初始化Metadata Pipeline根节点
	if err = zkutils.CreateNodeIfNecessary(path.GetPipelineMetadataRoot(), conn); err != nil {
		return err
	}
	// 初始化Metadata Rule根节点
	if err = zkutils.CreateNodeIfNecessary(path.GetRuleMetadataRoot(), conn); err != nil {
		return err
	}

	_connection = conn
	_connectionSignal = signal
	_addresses = addresses

	return nil
}

func Close() {
	if _connection != nil {
		_connection.Close()
	}
}

func GetConnection() *zk.Conn {
	return _connection
}

func GetConnectionSignal() <-chan zk.Event {
	return _connectionSignal
}

func GetAddresses() []string {
	return _addresses
}

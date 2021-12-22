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

package dao

import (
	"strings"
	"time"

	"github.com/go-zookeeper/zk"

	"go-mysql-transfer/config"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/util/logagent"
	"go-mysql-transfer/util/nodepath"
	"go-mysql-transfer/util/zkutils"
)

var (
	_zkConn       *zk.Conn
	_zkConnSignal <-chan zk.Event
	_zkAddrList   []string
)

func initZookeeper(config *config.AppConfig) error {
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
	err = zkutils.CreateNodeIfNecessary(nodepath.GetRootNode(), conn)
	if err != nil {
		return err
	}
	// 初始化State节点
	err = zkutils.CreateNodeIfNecessary(nodepath.GetStateParentNode(), conn)
	if err != nil {
		return err
	}
	// 初始化Position 根节点
	err = zkutils.CreateNodeIfNecessary(nodepath.GetPositionParentNode(), conn)
	if err != nil {
		return err
	}
	// 初始化Machine 根节点
	err = zkutils.CreateNodeIfNecessary(nodepath.GetMachineParentNode(), conn)
	if err != nil {
		return err
	}
	// 初始化Metadata 根节点
	err = zkutils.CreateNodeIfNecessary(nodepath.GetMetadataRootNode(), conn)
	if err != nil {
		return err
	}
	// 初始化Metadata Source根节点
	err = zkutils.CreateNodeIfNecessary(nodepath.GetMetadataParentNode(constants.MetadataTypeSource), conn)
	if err != nil {
		return err
	}
	// 初始化Metadata Endpoint根节点
	err = zkutils.CreateNodeIfNecessary(nodepath.GetMetadataParentNode(constants.MetadataTypeEndpoint), conn)
	if err != nil {
		return err
	}
	// 初始化Metadata Pipeline根节点
	err = zkutils.CreateNodeIfNecessary(nodepath.GetMetadataParentNode(constants.MetadataTypePipeline), conn)
	if err != nil {
		return err
	}

	_zkConn = conn
	_zkConnSignal = signal
	_zkAddrList = addresses

	return nil
}

func closeZookeeper() {
	if _zkConn != nil {
		_zkConn.Close()
	}
}

func GetZkConn() *zk.Conn {
	return _zkConn
}

func GetZkConnSignal() <-chan zk.Event {
	return _zkConnSignal
}

func GetZkAddrList() []string {
	return _zkAddrList
}

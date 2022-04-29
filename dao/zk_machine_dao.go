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
	"github.com/go-zookeeper/zk"

	"go-mysql-transfer/util/byteutils"
	"go-mysql-transfer/util/nodepath"
)

type ZkMachineDao struct {
}

func (s *ZkMachineDao) GetId(nodeName string) (uint16, error) {
	path := nodepath.GetMachineNode(nodeName)
	exist, _, err := _zkConn.Exists(path)
	if err != nil {
		return 0, err
	}

	if exist {
		var d []byte
		d, _, err = _zkConn.Get(path)
		if err != nil {
			return 0, err
		}
		return byteutils.BytesToUint16(d), nil
	}

	parentPath := nodepath.GetMachineParentNode()
	var ls []string
	ls, _, err = _zkConn.Children(parentPath)
	if err != nil {
		return 0, err
	}
	n := uint16(len(ls)) + 1
	data := byteutils.Uint16ToBytes(n)
	_, err = _zkConn.Create(path, data, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		return 0, err
	}

	return n, err
}

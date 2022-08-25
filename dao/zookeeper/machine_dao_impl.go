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
	"github.com/go-zookeeper/zk"

	"go-mysql-transfer/dao/path"
	"go-mysql-transfer/util/byteutils"
)

type MachineDaoImpl struct {
}

func (s *MachineDaoImpl) GetMachineIndex(machineUrl string) (uint16, error) {
	current := path.CreateMachinePath(machineUrl)
	exist, _, err := _connection.Exists(current)
	if err != nil {
		return 0, err
	}

	if exist {
		var data []byte
		data, _, err = _connection.Get(current)
		if err != nil {
			return 0, err
		}
		return byteutils.BytesToUint16(data), nil
	}

	machineRoot := path.GetMachineRoot()
	var ls []string
	ls, _, err = _connection.Children(machineRoot)
	if err != nil {
		return 0, err
	}
	index := uint16(len(ls)) + 1
	data := byteutils.Uint16ToBytes(index)
	_, err = _connection.Create(current, data, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		return 0, err
	}

	return index, err
}

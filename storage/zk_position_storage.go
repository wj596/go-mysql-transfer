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
package storage

import (
	"encoding/json"

	"github.com/siddontang/go-mysql/mysql"

	"go-mysql-transfer/global"
	"go-mysql-transfer/util/zookeepers"
)

type zkPositionStorage struct {
}

func (s *zkPositionStorage) Initialize() error {
	pos, err := json.Marshal(mysql.Position{})
	if err != nil {
		return err
	}

	err = zookeepers.CreateDirWithDataIfNecessary(global.Cfg().ZkPositionDir(), pos ,_zkConn)
	if err != nil {
		return err
	}

	err = zookeepers.CreateDirIfNecessary(global.Cfg().ZkNodesDir(), _zkConn)
	return err
}

func (s *zkPositionStorage) Save(pos mysql.Position) error {
	_, stat, err := _zkConn.Get(global.Cfg().ZkPositionDir())
	if err != nil {
		return err
	}

	data, err := json.Marshal(pos)
	if err != nil {
		return err
	}

	_, err = _zkConn.Set(global.Cfg().ZkPositionDir(), data, stat.Version)

	return err
}

func (s *zkPositionStorage) Get() (mysql.Position, error) {
	var entity mysql.Position

	data, _, err := _zkConn.Get(global.Cfg().ZkPositionDir())
	if err != nil {
		return entity, err
	}

	err = json.Unmarshal(data, &entity)

	return entity, err
}

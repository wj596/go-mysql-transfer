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

	"github.com/samuel/go-zookeeper/zk"
	"github.com/siddontang/go-mysql/mysql"

	"go-mysql-transfer/global"
)

type zkPositionStorage struct {
	Conf *global.Config
}

func (s *zkPositionStorage) Initialize() error {
	exist, _, err := _zkConn.Exists(s.Conf.ZePositionDir())
	if err != nil {
		return err
	}
	if !exist {
		data, err := json.Marshal(mysql.Position{})
		if err != nil {
			return err
		}
		_, err = _zkConn.Create(s.Conf.ZePositionDir(), data, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *zkPositionStorage) Save(pos mysql.Position) error {
	_, stat, err := _zkConn.Get(s.Conf.ZePositionDir())
	if err != nil {
		return err
	}

	data, err := json.Marshal(pos)
	if err != nil {
		return err
	}

	_, err = _zkConn.Set(s.Conf.ZePositionDir(), data, stat.Version)

	return err
}

func (s *zkPositionStorage) Get() (mysql.Position, error) {
	var entity mysql.Position

	data, _, err := _zkConn.Get(s.Conf.ZePositionDir())
	if err != nil {
		return entity, err
	}

	err = json.Unmarshal(data, &entity)

	return entity, err
}

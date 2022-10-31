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
	"encoding/json"
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-zookeeper/zk"

	"go-mysql-transfer/dao/path"
	"go-mysql-transfer/util/log"
)

type PositionDaoImpl struct {
	cache       map[uint64]bool
	lockOfCache sync.Mutex
}

func NewPositionDao() *PositionDaoImpl {
	return &PositionDaoImpl{
		cache: make(map[uint64]bool),
	}
}

func (s *PositionDaoImpl) Save(pipelineId uint64, position mysql.Position) error {
	if err := s.createPathIfNecessary(pipelineId); err != nil {
		return err
	}

	data, err := json.Marshal(&position)
	if err != nil {
		return err
	}

	node := path.CreatePositionPath(pipelineId)
	_, err = _connection.Set(node, data, -1)
	return err
}

func (s *PositionDaoImpl) Get(pipelineId uint64) mysql.Position {
	var entity mysql.Position

	node := path.CreatePositionPath(pipelineId)
	data, _, err := _connection.Get(node)
	if err != nil {
		log.Error(err.Error())
		return entity
	}

	err = json.Unmarshal(data, &entity)
	if err != nil {
		log.Error(err.Error())
	}
	return entity
}

func (s *PositionDaoImpl) createPathIfNecessary(pipelineId uint64) error {
	s.lockOfCache.Lock()
	defer s.lockOfCache.Unlock()

	if _, exist := s.cache[pipelineId]; exist {
		return nil
	}

	node := path.CreatePositionPath(pipelineId)
	exist, _, err := _connection.Exists(node)
	if err != nil {
		return err
	}
	if exist {
		s.cache[pipelineId] = true
		return nil
	}

	_, err = _connection.Create(node, nil, 0, zk.WorldACL(zk.PermAll))
	if err == nil {
		s.cache[pipelineId] = true
		return nil
	}

	return err
}

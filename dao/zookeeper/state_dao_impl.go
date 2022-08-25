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

	"github.com/go-zookeeper/zk"

	"go-mysql-transfer/dao/path"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/util/log"
)

type StateDaoImpl struct {
	cache       map[uint64]bool
	lockOfCache sync.Mutex
}

func NewStateDao() *StateDaoImpl {
	return &StateDaoImpl{
		cache: make(map[uint64]bool),
	}
}

func (s *StateDaoImpl) Save(pipelineId uint64, state *po.PipelineState) error {
	if err := s.createPathIfNecessary(pipelineId); err != nil {
		return err
	}

	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	node := path.CreateStatePath(pipelineId)
	_, err = _connection.Set(node, data, -1)
	return err
}

func (s *StateDaoImpl) IsExists(pipelineId uint64) (bool, error) {
	node := path.CreateStatePath(pipelineId)
	exist, _, err := _connection.Exists(node)
	if err != nil {
		log.Error(err.Error())
		return false, err
	}
	return exist, nil
}

func (s *StateDaoImpl) Get(pipelineId uint64) (*po.PipelineState, error) {
	node := path.CreateStatePath(pipelineId)
	data, _, err := _connection.Get(node)
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	var entity po.PipelineState
	err = json.Unmarshal(data, &entity)
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}
	return &entity, nil
}

func (s *StateDaoImpl) createPathIfNecessary(pipelineId uint64) error {
	s.lockOfCache.Lock()
	defer s.lockOfCache.Unlock()

	if _, exist := s.cache[pipelineId]; exist {
		return nil
	}

	node := path.CreateStatePath(pipelineId)
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

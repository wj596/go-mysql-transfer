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
	"encoding/json"
	"sync"

	"github.com/go-zookeeper/zk"
	"github.com/siddontang/go-mysql/mysql"

	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/nodepath"
)

type ZkPositionDao struct {
	positions map[uint64]bool
	lock      sync.Mutex
}

func (s *ZkPositionDao) Save(pipelineId uint64, position mysql.Position) error {
	if err := s.createPositionIfNecessary(pipelineId); err != nil {
		return err
	}

	data, err := json.Marshal(&position)
	if err != nil {
		return err
	}

	node := nodepath.GetPositionNode(pipelineId)
	_, err = _zkConn.Set(node, data, -1)
	return err
}

func (s *ZkPositionDao) Get(pipelineId uint64) mysql.Position {
	var entity mysql.Position

	node := nodepath.GetPositionNode(pipelineId)
	data, _, err := _zkConn.Get(node)
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

func (s *ZkPositionDao) createPositionIfNecessary(pipelineId uint64) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, exist := s.positions[pipelineId]; exist {
		return nil
	}

	node := nodepath.GetPositionNode(pipelineId)
	exist, _, err := _zkConn.Exists(node)
	if err != nil {
		return err
	}
	if exist {
		s.positions[pipelineId] = true
		return nil
	}

	_, err = _zkConn.Create(node, nil, 0, zk.WorldACL(zk.PermAll))
	if err == nil {
		s.positions[pipelineId] = true
		return nil
	}
	return err
}

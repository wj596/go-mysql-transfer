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
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/nodepath"
)

type ZkStateDao struct {
	states map[uint64]bool
	lock   sync.Mutex
}

func (s *ZkStateDao) Save(pipelineId uint64, state *po.PipelineState) error {
	if err := s.createStateIfNecessary(pipelineId); err != nil {
		return err
	}

	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	node := nodepath.GetStateNode(pipelineId)
	_, err = _zkConn.Set(node, data, -1)
	return err
}

func (s *ZkStateDao) Exists(pipelineId uint64) (bool, error) {
	node := nodepath.GetStateNode(pipelineId)
	exist, _, err := _zkConn.Exists(node)
	if err != nil {
		log.Error(err.Error())
		return false, err
	}
	return exist, nil
}

func (s *ZkStateDao) Get(pipelineId uint64) (*po.PipelineState, error) {
	node := nodepath.GetStateNode(pipelineId)
	data, _, err := _zkConn.Get(node)
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

func (s *ZkStateDao) createStateIfNecessary(pipelineId uint64) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, exist := s.states[pipelineId]; exist {
		return nil
	}

	node := nodepath.GetStateNode(pipelineId)
	exist, _, err := _zkConn.Exists(node)
	if err != nil {
		return err
	}
	if exist {
		s.states[pipelineId] = true
		return nil
	}

	_, err = _zkConn.Create(node, nil, 0, zk.WorldACL(zk.PermAll))
	if err == nil {
		s.states[pipelineId] = true
		return nil
	}
	return err
}

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

package mysql

import (
	"sync"

	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/util/log"
)

const (
	_countStateSql  = "SELECT COUNT(1) FROM T_STATE WHERE ID = ?"
	_selectStateSql = "SELECT ID, STATUS, INSERT_COUNT, UPDATE_COUNT, DELETE_COUNT, NODE, START_TIME, UPDATE_TIME FROM T_STATE WHERE ID = ?"
	_insertStateSql = "INSERT INTO T_STATE(ID, STATUS, INSERT_COUNT, UPDATE_COUNT, DELETE_COUNT, NODE, START_TIME, UPDATE_TIME) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
	_updateStateSql = "UPDATE T_STATE SET STATUS = ?, INSERT_COUNT = ?, UPDATE_COUNT = ?, DELETE_COUNT = ?, NODE = ?, UPDATE_TIME = ? WHERE ID = ?"
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
	if err := s.insertIfNecessary(pipelineId, state); err != nil {
		return err
	}

	_, err := _orm.Exec(_updateStateSql,
		state.Status,
		state.InsertCount,
		state.UpdateCount,
		state.DeleteCount,
		state.Node,
		state.UpdateTime,
		pipelineId)
	return err
}

func (s *StateDaoImpl) IsExists(pipelineId uint64) (bool, error) {
	var count int64
	_, err := _orm.SQL(_countStateSql, pipelineId).Get(&count)
	if err != nil {
		return false, err
	}
	if count > 0 {
		return true, nil
	}
	return false, err
}

func (s *StateDaoImpl) Get(pipelineId uint64) (*po.PipelineState, error) {
	var entity po.StateEntity
	_, err := _orm.SQL(_selectStateSql, pipelineId).Get(&entity)
	if err != nil {
		log.Error(err.Error())
	}

	return &po.PipelineState{
		Status:      entity.Status,
		InsertCount: entity.InsertCount,
		UpdateCount: entity.UpdateCount,
		DeleteCount: entity.DeleteCount,
		Node:        entity.Node,
		StartTime:   entity.StartTime,
		UpdateTime:  entity.UpdateTime,
	}, nil
}

func (s *StateDaoImpl) insertIfNecessary(pipelineId uint64, state *po.PipelineState) error {
	s.lockOfCache.Lock()
	defer s.lockOfCache.Unlock()

	if _, exist := s.cache[pipelineId]; exist {
		return nil
	}

	var count int64
	_, err := _orm.SQL(_countStateSql, pipelineId).Get(&count)
	if err != nil {
		return err
	}
	if count > 0 {
		s.cache[pipelineId] = true
		return nil
	}

	_, err = _orm.Exec(_insertStateSql,
		pipelineId,
		state.Status,
		state.InsertCount,
		state.UpdateCount,
		state.DeleteCount,
		state.Node,
		state.StartTime,
		state.UpdateTime)

	return err
}

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

	"github.com/go-mysql-org/go-mysql/mysql"

	"go-mysql-transfer/util/log"
)

const (
	_countPositionSql  = "SELECT COUNT(1) FROM T_POSITION WHERE ID = ?"
	_selectPositionSql = "SELECT ID, NAME, POS FROM T_POSITION WHERE ID = ?"
	_insertPositionSql = "INSERT INTO T_POSITION(ID, NAME, POS) VALUES (?, ?, ?)"
	_updatePositionSql = "UPDATE T_POSITION SET NAME = ?, POS = ? WHERE ID = ?"
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
	if err := s.initializeIfNecessary(pipelineId); err != nil {
		return err
	}
	_, err := _orm.Exec(_updatePositionSql, position.Name, position.Pos, pipelineId)
	return err
}

func (s *PositionDaoImpl) Get(pipelineId uint64) mysql.Position {
	var entity mysql.Position
	_, err := _orm.SQL(_selectPositionSql, pipelineId).Get(&entity)
	if err != nil {
		log.Error(err.Error())
	}
	return entity
}

func (s *PositionDaoImpl) initializeIfNecessary(pipelineId uint64) error {
	s.lockOfCache.Lock()
	defer s.lockOfCache.Unlock()

	if _, exist := s.cache[pipelineId]; exist {
		return nil
	}

	var count int64
	_, err := _orm.SQL(_countPositionSql, pipelineId).Get(&count)
	if err != nil {
		return err
	}
	if count > 0 {
		s.cache[pipelineId] = true
		return nil
	}

	_, err = _orm.Exec(_insertPositionSql, pipelineId, "", 0)
	return err
}

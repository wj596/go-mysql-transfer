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

package service

import (
	"github.com/siddontang/go-mysql/mysql"

	"go-mysql-transfer/dao"
	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/util/dateutils"
)

type PositionService struct {
	dao dao.PositionDao
}

func (s *PositionService) update(pipelineId uint64, pos mysql.Position) error {
	if pos.Name == "" && pos.Pos == 0 {
		return nil
	}

	err := s.dao.Save(pipelineId, pos)
	if err != nil {
		return err
	}

	var runtime *bo.PipelineRuntime
	runtime, err = _stateService.GetOrCreateRuntime(pipelineId)
	if err != nil {
		return err
	}

	runtime.PositionName.Store(pos.Name)
	runtime.PositionIndex.Store(pos.Pos)
	runtime.UpdateTime.Store(dateutils.NowMillisecond())

	return nil
}

func (s *PositionService) get(pipelineId uint64) mysql.Position {
	pos := s.dao.Get(pipelineId)
	runtime, exist := _stateService.getRuntime(pipelineId)
	if exist {
		runtime.PositionName.Store(pos.Name)
		runtime.PositionIndex.Store(pos.Pos)
		runtime.UpdateTime.Store(dateutils.NowMillisecond())
	}
	return pos
}

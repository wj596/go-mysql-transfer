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
	"sync"

	"go-mysql-transfer/dao"
	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/util/dateutils"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/stringutils"
)

type PipelineStateService struct {
	dao      dao.StateDao
	lock     sync.RWMutex
	runtimes map[uint64]*bo.PipelineRuntime
}

func (s *PipelineStateService) GetOrCreateRuntime(pipelineId uint64) (*bo.PipelineRuntime, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	runtime, exist := s.runtimes[pipelineId]
	if exist {
		return runtime, nil
	}

	pipeline, err := _pipelineInfoService.Get(pipelineId)
	if err != nil {
		return nil, err
	}

	var state *po.PipelineState
	exist, err = _stateService.existState(pipelineId)
	if err != nil {
		return nil, err
	}

	if exist {
		state, err = _stateService.GetState(pipelineId)
		if err != nil {
			return nil, err
		}
	} else {
		state, err = s.createState(pipelineId)
		if err != nil {
			return nil, err
		}
	}

	position := _positionService.dao.Get(pipelineId)

	runtime = bo.NewPipelineRuntime(pipelineId, pipeline.Name)
	runtime.InsertCounter.Store(state.InsertCount)
	runtime.UpdateCounter.Store(state.UpdateCount)
	runtime.DeleteCounter.Store(state.DeleteCount)
	runtime.PositionName.Store(position.Name)
	runtime.PositionIndex.Store(position.Pos)
	if IsCluster() {
		runtime.Node.Store(GetCurrNode())
	}

	s.runtimes[pipeline.Id] = runtime
	return runtime, nil
}

func (s *PipelineStateService) getRuntime(pipelineId uint64) (*bo.PipelineRuntime, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	runtime, exist := s.runtimes[pipelineId]
	return runtime, exist
}

func (s *PipelineStateService) existRuntime(pipelineId uint64) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()

	_, ok := s.runtimes[pipelineId]
	return ok
}

func (s *PipelineStateService) getBatchingRuntime() (*bo.PipelineRuntime, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	for _, runtime := range s.runtimes {
		if runtime.Status.Load() == constants.PipelineRunStatusBatching {
			return runtime, true
		}
	}

	return nil, false
}

func (s *PipelineStateService) existRunningRuntime() bool {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if len(s.runtimes) == 0 {
		return false
	}

	for _, runtime := range s.runtimes {
		if runtime.Status.Load() == constants.PipelineRunStatusRunning {
			return true
		}
	}
	return false
}

func (s *PipelineStateService) getRunningRuntimes() []*bo.PipelineRuntime {
	s.lock.RLock()
	defer s.lock.RUnlock()

	ls := make([]*bo.PipelineRuntime, 0)
	for _, runtime := range s.runtimes {
		if runtime.Status.Load() == constants.PipelineRunStatusRunning {
			ls = append(ls, runtime)
		}
	}
	return ls
}

func (s *PipelineStateService) removeRuntime(pipelineId uint64) {
	s.lock.Lock()
	defer s.lock.Unlock()

	delete(s.runtimes, pipelineId)
}

func (s *PipelineStateService) existState(pipelineId uint64) (bool, error) {
	return s.dao.Exists(pipelineId)
}

func (s *PipelineStateService) GetState(pipelineId uint64) (*po.PipelineState, error) {
	return s.dao.Get(pipelineId)
}

func (s *PipelineStateService) createState(pipelineId uint64) (*po.PipelineState, error) {
	state := &po.PipelineState{
		Status: constants.PipelineRunStatusInitial,
	}
	if IsCluster() {
		state.Node = GetCurrNode()
	}

	err := s.dao.Save(pipelineId, state)
	if err != nil {
		return nil, err
	}
	return state, nil
}

func (s *PipelineStateService) updateStateByFail(pipelineId uint64, runtime *bo.PipelineRuntime, cause string) {
	runtime.CleanBatchInfo()
	runtime.Status.Store(constants.PipelineRunStatusFail)
	runtime.LatestMessage.Store(cause)
	runtime.StartTime.Store(stringutils.Blank)
	runtime.UpdateTime.Store(0)
	s.updateState(pipelineId, runtime)
}

func (s *PipelineStateService) updateStateByClose(pipelineId uint64, runtime *bo.PipelineRuntime) {
	runtime.Status.Store(constants.PipelineRunStatusClose)
	runtime.LatestMessage.Store(stringutils.Blank)
	runtime.StartTime.Store(stringutils.Blank)
	runtime.UpdateTime.Store(0)
	s.updateState(pipelineId, runtime)
}

func (s *PipelineStateService) updateStateByPanic(pipelineId uint64, runtime *bo.PipelineRuntime, cause string) {
	runtime.Status.Store(constants.PipelineRunStatusPanic)
	runtime.LatestMessage.Store(cause)
	runtime.StartTime.Store(stringutils.Blank)
	runtime.UpdateTime.Store(0)
	s.updateState(pipelineId, runtime)
}

func (s *PipelineStateService) updateStateByRunning(pipelineId uint64, runtime *bo.PipelineRuntime) {
	runtime.CleanBatchInfo()
	runtime.StartTime.Store(dateutils.NowFormatted())
	runtime.Status.Store(constants.PipelineRunStatusRunning)
	runtime.LatestMessage.Store(stringutils.Blank)
	runtime.StartTime.Store(dateutils.NowFormatted())
	s.updateState(pipelineId, runtime)
}

func (s *PipelineStateService) updateStateByBatching(pipelineId uint64, runtime *bo.PipelineRuntime) {
	runtime.Status.Store(constants.PipelineRunStatusBatching)
	runtime.LatestMessage.Store(stringutils.Blank)
	runtime.BatchStartTime.Store(dateutils.NowFormatted())
	s.updateState(pipelineId, runtime)
}

func (s *PipelineStateService) updateStateByBatchEnd(pipelineId uint64, runtime *bo.PipelineRuntime) {
	runtime.Status.Store(constants.PipelineRunStatusBatchEnd)
	runtime.BatchEndTime.Store(dateutils.NowFormatted())
	s.updateState(pipelineId, runtime)
}

func (s *PipelineStateService) updateState(pipelineId uint64, runtime *bo.PipelineRuntime) {
	state := &po.PipelineState{
		Status:      runtime.Status.Load(),
		InsertCount: runtime.InsertCounter.Load(),
		UpdateCount: runtime.UpdateCounter.Load(),
		DeleteCount: runtime.DeleteCounter.Load(),
		UpdateTime:  dateutils.NowMillisecond(),
		Node:        GetCurrNode(),
	}

	err := s.dao.Save(pipelineId, state)
	if err != nil {
		log.Error(err.Error())
	}
	state = nil
}

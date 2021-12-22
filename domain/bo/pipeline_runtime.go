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

package bo

import (
	"math"

	"github.com/siddontang/go-mysql/canal"
	"go.uber.org/atomic"

	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/vo"
	"go-mysql-transfer/util/dateutils"
	"go-mysql-transfer/util/stringutils"
)

type PipelineRuntime struct {
	PipelineId          *atomic.Uint64
	PipelineName        *atomic.String
	Status              *atomic.Uint32
	Node                *atomic.String
	StartTime           *atomic.String
	UpdateTime          *atomic.Int64
	PositionName        *atomic.String
	PositionIndex       *atomic.Uint32
	InsertCounter       *atomic.Uint64
	UpdateCounter       *atomic.Uint64
	DeleteCounter       *atomic.Uint64
	LatestMessage       *atomic.String
	BatchStartTime      *atomic.String
	BatchEndTime        *atomic.String
	BatchTotalCounters  map[string]*atomic.Uint64
	BatchInsertCounters map[string]*atomic.Uint64
}

func NewPipelineRuntime(pipelineId uint64, pipelineName string) *PipelineRuntime {
	return &PipelineRuntime{
		PipelineId:     atomic.NewUint64(pipelineId),
		PipelineName:   atomic.NewString(pipelineName),
		Status:         atomic.NewUint32(0),
		Node:           atomic.NewString(""),
		StartTime:      atomic.NewString(""),
		UpdateTime:     atomic.NewInt64(0),
		PositionName:   atomic.NewString(""),
		PositionIndex:  atomic.NewUint32(0),
		InsertCounter:  atomic.NewUint64(0),
		UpdateCounter:  atomic.NewUint64(0),
		DeleteCounter:  atomic.NewUint64(0),
		LatestMessage:  atomic.NewString(""),
		BatchStartTime: atomic.NewString(""),
		BatchEndTime:   atomic.NewString(""),
	}
}

func (s *PipelineRuntime) SetCloseStatus() {
	s.Status.Store(constants.PipelineRunStatusClose)
	s.UpdateTime.Store(dateutils.NowMillisecond())
	s.LatestMessage.Store(stringutils.Blank)
}

func (s *PipelineRuntime) SetPanicStatus(cause string) {
	s.Status.Store(constants.PipelineRunStatusPanic)
	s.UpdateTime.Store(dateutils.NowMillisecond())
	s.LatestMessage.Store(cause)
}

func (s *PipelineRuntime) SetFaultStatus(cause string) {
	s.Status.Store(constants.PipelineRunStatusFault)
	s.LatestMessage.Store(cause)
}

func (s *PipelineRuntime) IsInitial() bool {
	return constants.PipelineRunStatusInitial == s.Status.Load()
}

func (s *PipelineRuntime) IsRunning() bool {
	return constants.PipelineRunStatusRunning == s.Status.Load()
}

func (s *PipelineRuntime) IsFault() bool {
	return constants.PipelineRunStatusFault == s.Status.Load()
}

func (s *PipelineRuntime) IsBatching() bool {
	return constants.PipelineRunStatusBatching == s.Status.Load()
}

func (s *PipelineRuntime) IsFail() bool {
	return constants.PipelineRunStatusFail == s.Status.Load()
}

func (s *PipelineRuntime) IsClose() bool {
	return constants.PipelineRunStatusClose == s.Status.Load()
}

func (s *PipelineRuntime) IsPanic() bool {
	return constants.PipelineRunStatusPanic == s.Status.Load()
}

func (s *PipelineRuntime) IsBatchEnd() bool {
	return constants.PipelineRunStatusBatchEnd == s.Status.Load()
}

func (s *PipelineRuntime) CleanBatchInfo() {
	s.BatchEndTime = atomic.NewString(stringutils.Blank)
	s.BatchStartTime = atomic.NewString(stringutils.Blank)
	s.BatchTotalCounters = nil
	s.BatchInsertCounters = nil
	s.UpdateTime.Store(dateutils.NowMillisecond())
}

func (s *PipelineRuntime) GetBatchTotalCounter(tableFullName string) *atomic.Uint64 {
	v, _ := s.BatchTotalCounters[tableFullName]
	return v
}

func (s *PipelineRuntime) GetBatchInsertCounter(tableFullName string) *atomic.Uint64 {
	v, _ := s.BatchInsertCounters[tableFullName]
	return v
}

func (s *PipelineRuntime) AddCount(action string, n int) {
	switch action {
	case canal.InsertAction:
		s.InsertCounter.Add(uint64(n))
	case canal.UpdateAction:
		s.UpdateCounter.Add(uint64(n))
	case canal.DeleteAction:
		s.DeleteCounter.Add(uint64(n))
	}
	s.UpdateTime.Store(dateutils.NowMillisecond())
}

func (s *PipelineRuntime) ToVO() *vo.PipelineRuntimeVO {
	v := new(vo.PipelineRuntimeVO)
	if s.PipelineId != nil {
		v.PipelineId = s.PipelineId.Load()
	}
	if s.PipelineName != nil {
		v.PipelineName = s.PipelineName.Load()
	}
	if s.Status != nil {
		v.Status = s.Status.Load()
	}
	if s.StartTime != nil {
		v.StartTime = s.StartTime.Load()
	}
	if s.UpdateTime != nil && s.UpdateTime.Load() > 0 {
		v.UpdateTime = dateutils.TimestampLayout(s.UpdateTime.Load()/1000, dateutils.DayTimeSecondFormatter)
	}
	if s.PositionName != nil {
		v.PositionName = s.PositionName.Load()
	}
	if s.PositionIndex != nil {
		v.PositionIndex = s.PositionIndex.Load()
	}
	if s.InsertCounter != nil {
		v.InsertCount = s.InsertCounter.Load()
	}
	if s.UpdateCounter != nil {
		v.UpdateCount = s.UpdateCounter.Load()
	}
	if s.DeleteCounter != nil {
		v.DeleteCount = s.DeleteCounter.Load()
	}

	if s.LatestMessage != nil {
		v.Message = s.LatestMessage.Load()
	}
	if s.Node != nil {
		v.Node = s.Node.Load()
	}
	if s.Status.Load() == constants.PipelineRunStatusBatching || s.Status.Load() == constants.PipelineRunStatusBatchEnd {
		if s.BatchStartTime != nil {
			v.BatchStartTime = s.BatchStartTime.Load()
		}
		if s.BatchEndTime != nil {
			v.BatchEndTime = s.BatchEndTime.Load()
		}
		var total uint64
		if s.BatchTotalCounters != nil {
			totalCounters := make([]*vo.CounterVO, 0, len(s.BatchTotalCounters))
			for key, val := range s.BatchTotalCounters {
				totalCounters = append(totalCounters, &vo.CounterVO{
					Table: key,
					Count: val.Load(),
				})
				total += val.Load()
			}
			v.TotalCounters = totalCounters
		}

		var insert uint64
		if s.BatchInsertCounters != nil {
			insertCounters := make([]*vo.CounterVO, 0, len(s.BatchInsertCounters))
			for key, val := range s.BatchInsertCounters {
				insertCounters = append(insertCounters, &vo.CounterVO{
					Table: key,
					Count: val.Load(),
				})
				insert += val.Load()
			}
			v.InsertCounters = insertCounters
		}

		if total > 0 {
			percent := math.Ceil(float64(insert) / float64(total) * 100.0)
			v.BatchPercent = percent
		}
	}

	return v
}

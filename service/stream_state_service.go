package service

import (
	"github.com/siddontang/go-mysql/mysql"

	"go-mysql-transfer/dao"
	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
)

type StreamStateService struct {
	dao *dao.StreamStateDao
}

func (s *StreamStateService) SavePosition(pipelineId uint64, pos mysql.Position) error {
	return s.dao.SavePosition(pipelineId, pos)
}

func (s *StreamStateService) GetPosition(pipelineId uint64) mysql.Position {
	return s.dao.GetPosition(pipelineId)
}

func (s *StreamStateService) SaveRunningStatus(pipelineId uint64, runtime *bo.PipelineRunState) error {
	return s.dao.SaveStreamState(pipelineId, po.StreamState{
		RunStatus:   constants.PipelineRunStatusRunning,
		InsertCount: runtime.GetInsertCount(),
		UpdateCount: runtime.GetUpdateCount(),
		DeleteCount: runtime.GetDeleteCount(),
	})
}

func (s *StreamStateService) SaveCeaseStatus(pipelineId uint64, runtime *bo.PipelineRunState) error {
	return s.dao.SaveStreamState(pipelineId, po.StreamState{
		RunStatus:   constants.PipelineRunStatusCease,
		InsertCount: runtime.GetInsertCount(),
		UpdateCount: runtime.GetUpdateCount(),
		DeleteCount: runtime.GetDeleteCount(),
	})
}

func (s *StreamStateService) SaveStreamCounts(pipelineId uint64, runtime *bo.PipelineRunState) error {
	return s.dao.SaveStreamState(pipelineId, po.StreamState{
		RunStatus:   runtime.GetStatus(),
		InsertCount: runtime.GetInsertCount(),
		UpdateCount: runtime.GetUpdateCount(),
		DeleteCount: runtime.GetDeleteCount(),
	})
}

func (s *StreamStateService) GetStreamState(pipelineId uint64) po.StreamState {
	return s.dao.GetStreamState(pipelineId)
}

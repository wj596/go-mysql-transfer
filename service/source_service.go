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
	"time"

	"go-mysql-transfer/dao"
	"go-mysql-transfer/datasource"
	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/domain/vo"
	"go-mysql-transfer/util/snowflake"
)

type SourceInfoService struct {
	dao *dao.SourceInfoDao
}

func (s *SourceInfoService) Insert(entity *po.SourceInfo) error {
	id, err := snowflake.NextId()
	if err != nil {
		return err
	}
	entity.Id = id

	if IsLeader() {
		err := s.dao.SyncInsert(entity)
		if err == nil {
			s.sendSyncEvent(entity.Id, 0)
		}
		return err
	}
	return s.dao.Save(entity)
}

func (s *SourceInfoService) Update(entity *po.SourceInfo) error {
	if IsLeader() {
		v, err := s.dao.SyncUpdate(entity)
		if err == nil {
			s.sendSyncEvent(entity.Id, v)
		}
		return err
	}
	return s.dao.Save(entity)
}

func (s *SourceInfoService) Delete(id uint64) error {
	if IsLeader() {
		err := s.dao.SyncDelete(id)
		if err == nil {
			s.sendSyncEvent(id, -1)
		}
		return err
	}
	return s.dao.Delete(id)
}

func (s *SourceInfoService) sendSyncEvent(id uint64, dataVersion int32) {
	_leaderService.sendEvent(&bo.SyncEvent{
		MetadataId:   id,
		MetadataType: constants.MetadataTypeSource,
		DataVersion:  dataVersion,
		Timestamp:    time.Now().Unix(),
	})
}

func (s *SourceInfoService) Get(id uint64) (*po.SourceInfo, error) {
	return s.dao.Get(id)
}

func (s *SourceInfoService) SelectList(params *vo.SourceInfoParams) ([]*po.SourceInfo, error) {
	return s.dao.SelectList(params)
}

func (s *SourceInfoService) SelectSchemaList(id uint64) ([]string, error) {
	ds, err := s.dao.Get(id)
	if err != nil {
		return nil, err
	}

	ls, err := datasource.SelectSchemaNameList(ds)
	if err != nil {
		return nil, err
	}

	return ls, nil
}

func (s *SourceInfoService) SelectTableList(id uint64, schemaName string) ([]string, error) {
	ds, err := s.dao.Get(id)
	if err != nil {
		return nil, err
	}

	ls, err := datasource.SelectTableNameList(ds, schemaName)
	if err != nil {
		return nil, err
	}

	return ls, nil
}

func (s *SourceInfoService) SelectTableInfo(id uint64, schemaName, tableName string) (*bo.TableInfo, error) {
	ds, err := s.dao.Get(id)
	if err != nil {
		return nil, err
	}

	result, err := datasource.SelectTableInfo(ds, schemaName, tableName)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (s *SourceInfoService) TestConnect(ds *po.SourceInfo) error {
	return datasource.TestConnect(ds)
}

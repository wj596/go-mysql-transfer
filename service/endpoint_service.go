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
	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/domain/vo"
	"go-mysql-transfer/endpoint"
	"go-mysql-transfer/util/snowflake"
)

type EndpointInfoService struct {
	dao *dao.CompositeEndpointDao
}

func (s *EndpointInfoService) Insert(entity *po.EndpointInfo) error {
	id, err := snowflake.NextId()
	if err != nil {
		return err
	}
	entity.Id = id

	if IsLeader() {
		err := s.dao.CascadeInsert(entity)
		if err == nil {
			s.sendSyncEvent(entity.Id, 0)
		}
		return err
	}
	return s.dao.Save(entity)
}

func (s *EndpointInfoService) Update(entity *po.EndpointInfo) error {
	if IsLeader() {
		v, err := s.dao.CascadeUpdate(entity)
		if err == nil {
			s.sendSyncEvent(entity.Id, v)
		}
		return err
	}
	return s.dao.Save(entity)
}

func (s *EndpointInfoService) Delete(id uint64) error {
	if IsLeader() {
		err := s.dao.CascadeDelete(id)
		if err == nil {
			s.sendSyncEvent(id, -1)
		}
		return err
	}
	return s.dao.Delete(id)
}

func (s *EndpointInfoService) sendSyncEvent(id uint64, dataVersion int32) {
	_leaderService.sendEvent(&bo.SyncEvent{
		Id:        id,
		Type:      constants.SyncEventTypeEndpoint,
		Version:   dataVersion,
		Timestamp: time.Now().Unix(),
	})
}

func (s *EndpointInfoService) Get(id uint64) (*po.EndpointInfo, error) {
	return s.dao.Get(id)
}

func (s *EndpointInfoService) GetByName(name string) (*po.EndpointInfo, error) {
	return s.dao.GetByName(name)
}

func (s *EndpointInfoService) SelectList(params *vo.EndpointInfoParams) ([]*po.EndpointInfo, error) {
	return s.dao.SelectList(params)
}

func (s *EndpointInfoService) TestLink(info *po.EndpointInfo) error {
	ins := endpoint.NewEndpoint(info)
	defer ins.Close()
	return ins.Connect()
}

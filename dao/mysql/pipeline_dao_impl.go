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
	"google.golang.org/protobuf/proto"

	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/util/log"
)

type PipelineDaoImpl struct {
	MetadataType int
	MetadataDao  *MetadataDao
}

func (s *PipelineDaoImpl) Insert(entity *po.PipelineInfo) error {
	marshaled, err := proto.Marshal(entity)
	if err != nil {
		return err
	}
	return s.MetadataDao.Insert(entity.Id, s.MetadataType, marshaled)
}

func (s *PipelineDaoImpl) Delete(id uint64) error {
	return s.MetadataDao.Delete(id)
}

func (s *PipelineDaoImpl) Update(entity *po.PipelineInfo, version int32) error {
	marshaled, err := proto.Marshal(entity)
	if err != nil {
		return err
	}
	return s.MetadataDao.Update(entity.Id, version, marshaled)
}

func (s *PipelineDaoImpl) GetDataVersion(id uint64) (int32, error) {
	return s.MetadataDao.GetDataVersion(id)
}

func (s *PipelineDaoImpl) Get(id uint64) (*po.PipelineInfo, error) {
	var metadata po.Metadata
	_, err := _orm.SQL(_selectMetadataSql, id).Get(&metadata)
	if err != nil {
		log.Error(err.Error())
	}

	var entity po.PipelineInfo
	err = proto.Unmarshal(metadata.Data, &entity)
	if err != nil {
		return nil, err
	}
	entity.DataVersion = metadata.Version

	return &entity, nil
}

func (s *PipelineDaoImpl) SelectAllDataVersion() ([]*po.MetadataVersion, error) {
	return s.MetadataDao.SelectAllDataVersion(s.MetadataType)
}

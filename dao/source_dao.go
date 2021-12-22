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
	"strings"

	"github.com/juju/errors"
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"

	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/domain/vo"
	"go-mysql-transfer/util/nodepath"
)

type SourceInfoDao struct {
}

func (s *SourceInfoDao) Save(entity *po.SourceInfo) error {
	return doSave(entity.Id, _sourceBucket, entity)
}

func (s *SourceInfoDao) SyncInsert(entity *po.SourceInfo) error {
	return doSyncInsert(entity.Id, _sourceBucket, constants.MetadataTypeSource, entity)
}

func (s *SourceInfoDao) SyncUpdate(entity *po.SourceInfo) (int32, error) {
	node := nodepath.GetMetadataNode(constants.MetadataTypeSource, entity.Id)
	version, err := _metadataDao.getDataVersion(node)
	if err != nil {
		return 0, err
	}
	entity.DataVersion = version + 1
	return entity.DataVersion, doSyncUpdate(entity.Id, _sourceBucket, version, constants.MetadataTypeSource, entity)
}

func (s *SourceInfoDao) Delete(id uint64) error {
	return doDelete(id, _sourceBucket)
}

func (s *SourceInfoDao) SyncDelete(id uint64) error {
	return doSyncDelete(id, _sourceBucket, constants.MetadataTypeSource)
}

func (s *SourceInfoDao) Get(id uint64) (*po.SourceInfo, error) {
	var entity po.SourceInfo
	err := _mdb.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_sourceBucket)
		data := bt.Get(marshalId(id))
		if data == nil {
			return errors.NotFoundf("SourceInfo")
		}
		return proto.Unmarshal(data, &entity)
	})

	if nil != err {
		return nil, err
	}

	return &entity, err
}

func (s *SourceInfoDao) SelectList(params *vo.SourceInfoParams) ([]*po.SourceInfo, error) {
	list := make([]*po.SourceInfo, 0)
	err := _mdb.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_sourceBucket)
		cursor := bt.Cursor()
		for k, v := cursor.Last(); k != nil; k, v = cursor.Prev() {
			var entity po.SourceInfo
			if err := proto.Unmarshal(v, &entity); err == nil {
				if params.Name != "" && !strings.Contains(entity.Name, params.Name) {
					continue
				}
				if params.Host != "" && !strings.Contains(entity.Host, params.Host) {
					continue
				}
				if params.Enable && entity.Status == constants.SourceInfoStatusDisable {
					continue
				}
				list = append(list, &entity)
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return list, err
}

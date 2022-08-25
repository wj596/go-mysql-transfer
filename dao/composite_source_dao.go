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
	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/util/byteutils"
	"go-mysql-transfer/util/log"
	"strings"

	"github.com/juju/errors"
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"

	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/domain/vo"
)

type CompositeSourceDao struct {
}

func (s *CompositeSourceDao) Save(entity *po.SourceInfo) error {
	return _local.Update(func(tx *bbolt.Tx) error {
		data, err := proto.Marshal(entity)
		if err != nil {
			return err
		}
		return tx.Bucket(_sourceBucket).Put(marshalId(entity.Id), data)
	})
}

func (s *CompositeSourceDao) CascadeInsert(entity *po.SourceInfo) error {
	return _local.Update(func(tx *bbolt.Tx) error {
		data, err := proto.Marshal(entity)
		if err != nil {
			return err
		}

		err = tx.Bucket(_sourceBucket).Put(marshalId(entity.Id), data)
		if err != nil {
			return err
		}

		err = _remoteSourceDao.Insert(entity.Id, data)
		return err
	})
}

func (s *CompositeSourceDao) CascadeUpdate(entity *po.SourceInfo) (int32, error) {
	version, err := _remoteSourceDao.GetDataVersion(entity.Id)
	if err != nil {
		return 0, err
	}
	entity.DataVersion = version + 1

	err = _local.Update(func(tx *bbolt.Tx) error {
		data, err := proto.Marshal(entity)
		if err != nil {
			return err
		}

		err = tx.Bucket(_sourceBucket).Put(marshalId(entity.Id), data)
		if err != nil {
			return err
		}

		err = _remoteSourceDao.Update(entity.Id, version, data)
		return err
	})

	if err != nil {
		return version, err
	}
	return entity.DataVersion, nil
}

func (s *CompositeSourceDao) Delete(id uint64) error {
	return _local.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(_sourceBucket).Delete(marshalId(id))
	})
}

func (s *CompositeSourceDao) CascadeDelete(id uint64) error {
	return _local.Update(func(tx *bbolt.Tx) error {
		err := tx.Bucket(_sourceBucket).Delete(marshalId(id))
		if err != nil {
			return err
		}
		return _remoteSourceDao.Delete(id)
	})
}

func (s *CompositeSourceDao) Get(id uint64) (*po.SourceInfo, error) {
	var entity po.SourceInfo
	err := _local.View(func(tx *bbolt.Tx) error {
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

func (s *CompositeSourceDao) SelectList(params *vo.SourceInfoParams) ([]*po.SourceInfo, error) {
	list := make([]*po.SourceInfo, 0)
	err := _local.View(func(tx *bbolt.Tx) error {
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

func (s *CompositeSourceDao) SelectAllIdList() ([]uint64, error) {
	ids := make([]uint64, 0)
	err := _local.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_sourceBucket)
		cursor := bt.Cursor()
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			ids = append(ids, byteutils.BytesToUint64(k))
		}
		return nil
	})
	return ids, err
}

func (s *CompositeSourceDao) refreshAll(remoteNodes []*bo.NodeInfo) error {
	localIds := make([]uint64, 0)
	_local.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_sourceBucket)
		cursor := bt.Cursor()
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			localIds = append(localIds, byteutils.BytesToUint64(k))
		}
		return nil
	})

	delIds := make([]uint64, 0)
	for _, localId := range localIds {
		isRemove := true
		for _, remote := range remoteNodes {
			if localId == remote.Id {
				isRemove = false
				break
			}
		}
		if isRemove {
			delIds = append(delIds, localId)
		}
	}
	err := _local.Update(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_sourceBucket)
		for _, delId := range delIds {
			if err := bt.Delete(marshalId(delId)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	counter := 0
	for _, remoteNode := range remoteNodes {
		var localEntity *po.SourceInfo
		localEntity, _ = s.Get(remoteNode.Id)
		if nil != localEntity && localEntity.DataVersion >= remoteNode.Version {
			log.Infof("忽略刷新本地[SourceInfo]数据[1]条, id[%d], 名称[%s], 数据版本[%d:%d]", remoteNode.Id, localEntity.Name, localEntity.DataVersion, remoteNode.Version)
			continue
		}

		var remoteEntity *po.SourceInfo
		remoteEntity, err = _remoteSourceDao.Get(remoteNode.Id)
		if err != nil {
			return err
		}
		log.Infof("刷新本地[SourceInfo]数据[1]条, id[%d], 名称[%s], 数据版本[%d:%d]", remoteNode.Id, localEntity.Name, localEntity.DataVersion, remoteNode.Version)
		if err = s.Save(remoteEntity); err != nil {
			return err
		}
		counter++
	}

	log.Infof("共刷新本地[SourceInfo]数据[%d]条", counter)

	return nil
}

func (s *CompositeSourceDao) refreshOne(id uint64, currentVersion int32) error {
	remoteEntity, err := _remoteSourceDao.Get(id)
	if err != nil {
		return err
	}
	remoteExist := false
	if nil != remoteEntity {
		remoteExist = true
	}

	localExist := false
	localVersion := int32(-1)
	localEntity, _ := s.Get(id)
	if nil != localEntity {
		localExist = true
		localVersion = localEntity.DataVersion
	}

	if !remoteExist && localExist {
		log.Infof("删除本地[SourceInfo]数据[1]条, id[%d], 名称[%s], 数据版本[%d]", id, localEntity.Name, currentVersion)
		return s.Delete(id)
	}

	if remoteExist && localVersion < currentVersion {
		log.Infof("刷新本地[SourceInfo]数据[1]条, id[%d], 名称[%s], 数据版本[%d-%d]", id, remoteEntity.Name, localVersion, currentVersion)
		return s.Save(remoteEntity)
	}

	return nil
}

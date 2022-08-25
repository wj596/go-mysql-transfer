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
	"go-mysql-transfer/util/log"
)

type CompositeEndpointDao struct {
}

func (s *CompositeEndpointDao) Save(entity *po.EndpointInfo) error {
	return _local.Update(func(tx *bbolt.Tx) error {
		data, err := proto.Marshal(entity)
		if err != nil {
			return err
		}
		return tx.Bucket(_endpointBucket).Put(marshalId(entity.Id), data)
	})
}

func (s *CompositeEndpointDao) CascadeInsert(entity *po.EndpointInfo) error {
	return _local.Update(func(tx *bbolt.Tx) error {
		data, err := proto.Marshal(entity)
		if err != nil {
			return err
		}

		err = tx.Bucket(_endpointBucket).Put(marshalId(entity.Id), data)
		if err != nil {
			return err
		}

		err = _remoteEndpointDao.Insert(entity.Id, data)
		return err
	})
}

func (s *CompositeEndpointDao) CascadeUpdate(entity *po.EndpointInfo) (int32, error) {
	version, err := _remoteEndpointDao.GetDataVersion(entity.Id)
	if err != nil {
		return 0, err
	}
	entity.DataVersion = version + 1

	err = _local.Update(func(tx *bbolt.Tx) error {
		data, err := proto.Marshal(entity)
		if err != nil {
			return err
		}

		err = tx.Bucket(_endpointBucket).Put(marshalId(entity.Id), data)
		if err != nil {
			return err
		}

		err = _remoteEndpointDao.Update(entity.Id, version, data)
		return err
	})

	if err != nil {
		return version, err
	}
	return entity.DataVersion, nil
}

func (s *CompositeEndpointDao) Delete(id uint64) error {
	return _local.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(_endpointBucket).Delete(marshalId(id))
	})
}

func (s *CompositeEndpointDao) CascadeDelete(id uint64) error {
	return _local.Update(func(tx *bbolt.Tx) error {
		err := tx.Bucket(_endpointBucket).Delete(marshalId(id))
		if err != nil {
			return err
		}
		return _remoteEndpointDao.Delete(id)
	})
}

func (s *CompositeEndpointDao) Get(id uint64) (*po.EndpointInfo, error) {
	var entity po.EndpointInfo
	err := _local.View(func(tx *bbolt.Tx) error {
		data := tx.Bucket(_endpointBucket).Get(marshalId(id))
		if data == nil {
			return errors.NotFoundf("EndpointInfo")
		}
		return proto.Unmarshal(data, &entity)
	})

	if nil != err {
		return nil, err
	}
	return &entity, err
}

func (s *CompositeEndpointDao) GetByName(name string) (*po.EndpointInfo, error) {
	var entity po.EndpointInfo
	var found bool
	err := _local.View(func(tx *bbolt.Tx) error {
		cursor := tx.Bucket(_endpointBucket).Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			if err := proto.Unmarshal(v, &entity); err == nil {
				if name == entity.Name {
					found = true
					break
				}
			}
		}
		return nil
	})

	if err != nil {
		log.Errorf(err.Error())
		return nil, err
	}
	if !found {
		return nil, errors.NotFoundf("EndpointInfo")
	}
	return &entity, err
}

func (s *CompositeEndpointDao) SelectList(params *vo.EndpointInfoParams) ([]*po.EndpointInfo, error) {
	list := make([]*po.EndpointInfo, 0)
	err := _local.View(func(tx *bbolt.Tx) error {
		cursor := tx.Bucket(_endpointBucket).Cursor()
		for k, v := cursor.Last(); k != nil; k, v = cursor.Prev() {
			var entity po.EndpointInfo
			if err := proto.Unmarshal(v, &entity); err == nil {
				if params.Name != "" && !strings.Contains(entity.Name, params.Name) {
					continue
				}
				if params.Host != "" && !strings.Contains(entity.Addresses, params.Host) {
					continue
				}
				if params.Enable && entity.Status == constants.EndpointInfoStatusDisable {
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

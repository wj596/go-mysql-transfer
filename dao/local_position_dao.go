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
	"encoding/json"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"
	"go.etcd.io/bbolt"
)

type LocalPositionDao struct {
}

func (s *LocalPositionDao) Save(pipelineId uint64, pos mysql.Position) error {
	return _local.Update(func(tx *bbolt.Tx) error {
		data, err := json.Marshal(&pos)
		if err != nil {
			return err
		}
		return tx.Bucket(_positionBucket).Put(marshalId(pipelineId), data)
	})
}

func (s *LocalPositionDao) Get(pipelineId uint64) mysql.Position {
	var entity mysql.Position
	_local.View(func(tx *bbolt.Tx) error {
		data := tx.Bucket(_positionBucket).Get(marshalId(pipelineId))
		if data == nil {
			return errors.NotFoundf("Position")
		}
		return json.Unmarshal(data, &entity)
	})
	return entity
}

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
	"go.etcd.io/bbolt"

	"go-mysql-transfer/domain/po"
)

type BoltStateDao struct {
}

func (s *BoltStateDao) Save(pipelineId uint64, state *po.PipelineState) error {
	return _mdb.Update(func(tx *bbolt.Tx) error {
		data, err := json.Marshal(state)
		if err != nil {
			return err
		}
		return tx.Bucket(_stateBucket).Put(marshalId(pipelineId), data)
	})
}

func (s *BoltStateDao) Exists(pipelineId uint64) (bool, error) {
	var exist bool
	err := _mdb.View(func(tx *bbolt.Tx) error {
		data := tx.Bucket(_stateBucket).Get(marshalId(pipelineId))
		if data != nil {
			exist = true
		}
		return nil
	})
	return exist, err
}

func (s *BoltStateDao) Get(pipelineId uint64) (*po.PipelineState, error) {
	var entity po.PipelineState
	err := _mdb.View(func(tx *bbolt.Tx) error {
		data := tx.Bucket(_stateBucket).Get(marshalId(pipelineId))
		if data == nil {
			return errors.NotFoundf("PipelineState")
		}
		return json.Unmarshal(data, &entity)
	})

	if nil != err {
		return nil, err
	}

	return &entity, err
}

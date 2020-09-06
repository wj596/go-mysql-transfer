/*
 * Copyright 2020-2021 the original author(https://github.com/wj596)
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
package storage

import (
	"github.com/juju/errors"
	"go.etcd.io/bbolt"

	"go-mysql-transfer/util/byteutil"
)

type BoltRowStorage struct {
}

func (s *BoltRowStorage) Size() int {
	var size int
	_bolt.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_rowRequestBucket)
		size = bt.Stats().KeyN
		return nil
	})

	return size
}

func (s *BoltRowStorage) Add(data []byte) {
	_bolt.Update(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_rowRequestBucket)
		seq, _ := bt.NextSequence()
		return bt.Put(byteutil.Uint64ToBytes(seq), data)
	})
}

func (s *BoltRowStorage) BatchAdd(list [][]byte) {
	_bolt.Batch(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_rowRequestBucket)
		for _, data := range list {
			seq, _ := bt.NextSequence()
			bt.Put(byteutil.Uint64ToBytes(seq), data)
		}
		return nil
	})
}

func (s *BoltRowStorage) IdList() []uint64 {
	ls := make([]uint64, 0)
	_bolt.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_rowRequestBucket)
		cursor := bt.Cursor()
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			ls = append(ls, byteutil.BytesToUint64(k))
		}
		return nil
	})

	return ls
}

func (s *BoltRowStorage) Get(key uint64) ([]byte, error) {
	var entity []byte
	err := _bolt.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_rowRequestBucket)
		data := bt.Get(byteutil.Uint64ToBytes(key))
		if data == nil {
			return errors.NotFoundf("Row")
		}

		entity = data
		return nil
	})

	if err != nil {
		return nil, err
	}

	return entity, nil
}

func (s *BoltRowStorage) Delete(key uint64) error {
	return _bolt.Update(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_rowRequestBucket)
		return bt.Delete(byteutil.Uint64ToBytes(key))
	})
}

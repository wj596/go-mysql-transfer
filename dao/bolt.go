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
	"encoding/binary"
	"fmt"
	"path/filepath"

	"github.com/juju/errors"
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"

	"go-mysql-transfer/config"
	"go-mysql-transfer/util/byteutil"
	"go-mysql-transfer/util/fileutils"
	"go-mysql-transfer/util/nodepath"
)

const (
	_storePath        = "db"
	_fileMode         = 0600
	_metadataFileName = "metadata.db"
)

var (
	_mdb *bbolt.DB

	_sourceBucket   = []byte("source")
	_endpointBucket = []byte("endpoint")
	_pipelineBucket = []byte("pipeline")

	_positionBucket = []byte("position")
	_stateBucket    = []byte("state")
)

func initBolt(config *config.AppConfig) error {
	storePath := filepath.Join(config.GetDataDir(), _storePath)
	if err := fileutils.MkdirIfNecessary(storePath); err != nil {
		return errors.New(fmt.Sprintf("create metadataFilePath : %s", err.Error()))
	}

	var err error

	metadataFilePath := filepath.Join(storePath, _metadataFileName)
	_mdb, err = bbolt.Open(metadataFilePath, _fileMode, bbolt.DefaultOptions)
	if err != nil {
		return errors.New(fmt.Sprintf("open boltdb: %s", err.Error()))
	}

	err = _mdb.Update(func(tx *bbolt.Tx) error {
		if _, err = tx.CreateBucketIfNotExists(_sourceBucket); err != nil {
			return err
		}
		if _, err = tx.CreateBucketIfNotExists(_endpointBucket); err != nil {
			return err
		}
		if _, err = tx.CreateBucketIfNotExists(_pipelineBucket); err != nil {
			return err
		}
		if _, err = tx.CreateBucketIfNotExists(_positionBucket); err != nil {
			return err
		}

		if _, err = tx.CreateBucketIfNotExists(_positionBucket); err != nil {
			return err
		}
		if _, err = tx.CreateBucketIfNotExists(_stateBucket); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return errors.New(fmt.Sprintf("create bucket: %s", err.Error()))
	}

	return nil
}

func closeBolt() {
	if _mdb != nil {
		_mdb.Close()
	}
}

func marshalId(id uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, id)
	return buf
}

//func getBucket(metadataType string) []byte {
//	switch metadataType {
//	case constants.MetadataTypeSource:
//		return _sourceBucket
//	case constants.MetadataTypeEndpoint:
//		return _endpointBucket
//	case constants.MetadataTypePipeline:
//		return _pipelineBucket
//	}
//	return nil
//}

func doSelectIdList(bucket []byte) ([]uint64, error) {
	ids := make([]uint64, 0)
	err := _mdb.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(bucket)
		cursor := bt.Cursor()
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			ids = append(ids, byteutil.BytesToUint64(k))
		}
		return nil
	})
	return ids, err
}

func doSave(id uint64, bucket []byte, entity proto.Message) error {
	return _mdb.Update(func(tx *bbolt.Tx) error {
		data, err := proto.Marshal(entity)
		if err != nil {
			return err
		}
		return tx.Bucket(bucket).Put(marshalId(id), data)
	})
}

func doSaveBinary(id uint64, bucket []byte, data []byte) error {
	return _mdb.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucket).Put(marshalId(id), data)
	})
}

func doSyncInsert(id uint64, bucket []byte, metadataType string, entity proto.Message) error {
	return _mdb.Update(func(tx *bbolt.Tx) error {
		data, err := proto.Marshal(entity)
		if err != nil {
			return err
		}

		err = tx.Bucket(bucket).Put(marshalId(id), data)
		if err != nil {
			return err
		}

		node := nodepath.GetMetadataNode(metadataType, id)
		return _metadataDao.insert(node, data)
	})
}

func doSyncUpdate(id uint64, bucket []byte, version int32, metadataType string, entity proto.Message) error {
	return _mdb.Update(func(tx *bbolt.Tx) error {
		data, err := proto.Marshal(entity)
		if err != nil {
			return err
		}

		err = tx.Bucket(bucket).Put(marshalId(id), data)
		if err != nil {
			return err
		}

		node := nodepath.GetMetadataNode(metadataType, id)
		return _metadataDao.update(node, data, version)
	})
}

func doDelete(id uint64, bucket []byte) error {
	return _mdb.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucket).Delete(marshalId(id))
	})
}

func doSyncDelete(id uint64, bucket []byte, metadataType string) error {
	return _mdb.Update(func(tx *bbolt.Tx) error {
		err := tx.Bucket(bucket).Delete(marshalId(id))
		if err != nil {
			return err
		}
		node := nodepath.GetMetadataNode(metadataType, id)
		return _metadataDao.delete(node)
	})
}

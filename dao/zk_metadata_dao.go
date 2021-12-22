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
	"github.com/go-zookeeper/zk"

	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/util/gziputils"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/nodepath"
	"go-mysql-transfer/util/stringutils"
)

type ZkMetadataDao struct {
}

func (s *ZkMetadataDao) insert(node string, data []byte) error {
	gzip, err := gziputils.Zip(data)
	if err != nil {
		return err
	}

	_, err = _zkConn.Create(node, gzip, 0, zk.WorldACL(zk.PermAll))
	return err
}

func (s *ZkMetadataDao) delete(node string) error {
	return _zkConn.Delete(node, -1)
}

func (s *ZkMetadataDao) update(node string, data []byte, version int32) error {
	gzip, err := gziputils.Zip(data)
	if err != nil {
		return err
	}

	_, err = _zkConn.Set(node, gzip, version)
	return err
}

func (s *ZkMetadataDao) getDataVersion(node string) (int32, error) {
	exist, state, err := _zkConn.Exists(node)
	if err != nil {
		return 0, err
	}
	if exist {
		return state.Version, nil
	}

	return 0, nil
}

func (s *ZkMetadataDao) get(node string) ([]byte, int32, error) {
	data, stat, err := _zkConn.Get(node)
	if err != nil {
		return nil, 0, err
	}

	var ret []byte
	ret, err = gziputils.UnZip(data)
	return ret, stat.Version, err
}

func (s *ZkMetadataDao) SyncOne(metadataType string, id uint64) error {
	node := nodepath.GetMetadataNode(metadataType, id)
	exist, st, err := _zkConn.Exists(node)
	if err != nil {
		log.Errorf("同步[%s]元数据失败[%s]", metadataType, err.Error())
		return err
	}

	var bucket []byte
	var name string
	dataVersion := int32(-1)
	switch metadataType {
	case constants.MetadataTypeSource:
		bucket = _sourceBucket
		if entity, _ := _sourceInfoDao.Get(id); entity != nil {
			name = entity.Name
			dataVersion = entity.DataVersion
		}
	case constants.MetadataTypeEndpoint:
		bucket = _endpointBucket
		if entity, _ := _endpointInfoDao.Get(id); entity != nil {
			name = entity.Name
			dataVersion = entity.DataVersion
		}
	case constants.MetadataTypePipeline:
		bucket = _pipelineBucket
		if entity, _ := _pipelineInfoDao.Get(id); entity != nil {
			name = entity.Name
			dataVersion = entity.DataVersion
		}
	}

	if !exist && name != "" {
		log.Infof("[%s]元数据, id[%d], 名称[%s], 数据版本[%d], 删除", metadataType, id, name, dataVersion)
		return doDelete(id, bucket)
	}

	if exist && dataVersion < st.Version {
		var data []byte
		data, _, err = _metadataDao.get(node)
		if err != nil {
			log.Errorf("同步[%s]元数据失败[%s]", metadataType, err.Error())
			return err
		}
		log.Infof("[%s]元数据, id[%d], 名称[%s], 数据版本[%d-%d], 同步方式为更新", metadataType, id, name, dataVersion, st.Version)
		return doSaveBinary(id, bucket, data)
	}

	log.Infof("[%s]元数据, id[%d], 名称[%s], 数据版本[%d-%d], 无需同步", metadataType, id, name, dataVersion, st.Version)
	return nil
}

func (s *ZkMetadataDao) SyncAll() {
	err := s.doSyncAll(constants.MetadataTypeSource, _sourceBucket)
	if err != nil {
		log.Errorf("同步数据失败[%s]", err.Error())
	}
	err = s.doSyncAll(constants.MetadataTypeEndpoint, _endpointBucket)
	if err != nil {
		log.Errorf("同步数据失败[%s]", err.Error())
	}
	err = s.doSyncAll(constants.MetadataTypePipeline, _pipelineBucket)
	if err != nil {
		log.Errorf("同步数据失败[%s]", err.Error())
	}
}

func (s *ZkMetadataDao) doSyncAll(metadataType string, bucket []byte) error {
	parentNode := nodepath.GetMetadataParentNode(metadataType)
	keys, _, err := _zkConn.Children(parentNode)
	if err != nil {
		log.Errorf("同步[%s]元数据失败[%s]", metadataType, err.Error())
		return err
	}

	log.Infof("元数据[%s]共[%d]条", metadataType, len(keys))

	var ids []uint64
	ids, err = doSelectIdList(bucket)
	if err != nil {
		log.Errorf("同步[%s]元数据失败[%s]", metadataType, err.Error())
		return err
	}
	for _, id := range ids {
		needRemove := true
		for _, key := range keys {
			if stringutils.ToUint64Safe(key) == id {
				needRemove = false
				break
			}
		}
		if needRemove {
			doDelete(id, bucket)
			log.Errorf("同步[%s]元数据, 删除[%d]", metadataType, id)
		}
	}

	for _, key := range keys {
		err = s.SyncOne(metadataType, stringutils.ToUint64Safe(key))
		if err != nil {
			log.Errorf("同步[%s]元数据失败[%s]", metadataType, err.Error())
		}
	}

	return nil
}

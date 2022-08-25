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

package zookeeper

import (
	"github.com/go-zookeeper/zk"
	"google.golang.org/protobuf/proto"

	"go-mysql-transfer/dao/path"
	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/util/gziputils"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/stringutils"
)

type SourceDaoImpl struct {
}

func (s *SourceDaoImpl) Insert(id uint64, marshaled []byte) error {
	gzip, err := gziputils.Zip(marshaled)
	if err != nil {
		return err
	}

	node := path.CreateSourceMetadataPath(id)
	_, err = _connection.Create(node, gzip, 0, zk.WorldACL(zk.PermAll))
	return err
}

func (s *SourceDaoImpl) Delete(id uint64) error {
	node := path.CreateSourceMetadataPath(id)
	return _connection.Delete(node, -1)
}

func (s *SourceDaoImpl) Update(id uint64, version int32, marshaled []byte) error {
	gzip, err := gziputils.Zip(marshaled)
	if err != nil {
		return err
	}

	node := path.CreateSourceMetadataPath(id)
	_, err = _connection.Set(node, gzip, version)
	return err
}

func (s *SourceDaoImpl) GetDataVersion(id uint64) (int32, error) {
	node := path.CreateSourceMetadataPath(id)
	exist, state, err := _connection.Exists(node)
	if err != nil {
		return 0, err
	}
	if exist {
		return state.Version, nil
	}
	return 0, nil
}

func (s *SourceDaoImpl) Get(id uint64) (*po.SourceInfo, error) {
	node := path.CreateSourceMetadataPath(id)
	temp, stat, err := _connection.Get(node)
	if err != nil {
		return nil, err
	}

	var data []byte
	data, err = gziputils.UnZip(temp)
	if err != nil {
		return nil, err
	}

	var entity po.SourceInfo
	err = proto.Unmarshal(data, &entity)
	if err != nil {
		return nil, err
	}
	entity.DataVersion = stat.Version

	return &entity, nil
}

func (s *SourceDaoImpl) SelectAllNodeInfo() ([]*bo.NodeInfo, error) {
	root := path.GetSourceMetadataRoot()
	keys, _, err := _connection.Children(root)
	if err != nil {
		log.Errorf("查询所有[SourceInfo]节点失败[%s]", err.Error())
		return nil, err
	}

	ls := make([]*bo.NodeInfo, 0)
	for _, key := range keys {
		node := path.GetSourceMetadataRoot() + "/" + key
		_, stat, err := _connection.Exists(node)
		if err != nil {
			return nil, err
		}

		ls = append(ls, &bo.NodeInfo{
			Id:      stringutils.ToUint64Safe(key),
			Version: stat.Version,
		})
	}

	return ls, nil
}

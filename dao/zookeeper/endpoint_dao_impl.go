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
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/util/gziputils"
)

type EndpointDaoImpl struct {
}

func (s *EndpointDaoImpl) Insert(id uint64, marshaled []byte) error {
	gzip, err := gziputils.Zip(marshaled)
	if err != nil {
		return err
	}

	node := path.CreateEndpointMetadataPath(id)
	_, err = _connection.Create(node, gzip, 0, zk.WorldACL(zk.PermAll))
	return err
}

func (s *EndpointDaoImpl) Delete(id uint64) error {
	node := path.CreateEndpointMetadataPath(id)
	return _connection.Delete(node, -1)
}

func (s *EndpointDaoImpl) Update(id uint64, version int32, marshaled []byte) error {
	gzip, err := gziputils.Zip(marshaled)
	if err != nil {
		return err
	}

	node := path.CreateEndpointMetadataPath(id)
	_, err = _connection.Set(node, gzip, version)
	return err
}

func (s *EndpointDaoImpl) GetDataVersion(id uint64) (int32, error) {
	node := path.CreateEndpointMetadataPath(id)
	exist, state, err := _connection.Exists(node)
	if err != nil {
		return 0, err
	}
	if exist {
		return state.Version, nil
	}
	return 0, nil
}

func (s *EndpointDaoImpl) Get(id uint64) (*po.EndpointInfo, int32, error) {
	node := path.CreateEndpointMetadataPath(id)
	temp, stat, err := _connection.Get(node)
	if err != nil {
		return nil, stat.Version, err
	}

	var data []byte
	data, err = gziputils.UnZip(temp)
	if err != nil {
		return nil, stat.Version, err
	}

	var entity po.EndpointInfo
	err = proto.Unmarshal(data, &entity)
	if err != nil {
		return nil, stat.Version, err
	}

	return &entity, stat.Version, nil
}

func (s *EndpointDaoImpl) SelectAll() ([]*po.EndpointInfo, error) {
	var ls []*po.EndpointInfo
	return ls, nil
}

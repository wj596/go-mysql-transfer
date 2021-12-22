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
	"github.com/siddontang/go-mysql/mysql"

	"go-mysql-transfer/config"
	"go-mysql-transfer/domain/po"
)

var (
	_sourceInfoDao   *SourceInfoDao
	_endpointInfoDao *EndpointInfoDao
	_pipelineInfoDao *PipelineInfoDao
	_positionDao     PositionDao
	_stateDao        StateDao
	_machineDao      MachineDao
	_metadataDao     MetadataDao
)

type MachineDao interface {
	GetId(nodeName string) (uint16, error)
}

type MetadataDao interface {
	insert(node string, data []byte) error
	delete(node string) error
	update(node string, data []byte, version int32) error
	getDataVersion(node string) (int32, error)
	get(node string) ([]byte, int32, error)
	SyncAll()
	SyncOne(metadataType string, id uint64) error
}

type PositionDao interface {
	Save(pipelineId uint64, pos mysql.Position) error
	Get(pipelineId uint64) mysql.Position
}

type StateDao interface {
	Save(pipelineId uint64, state *po.PipelineState) error
	Exists(pipelineId uint64) (bool, error)
	Get(pipelineId uint64) (*po.PipelineState, error)
}

func Initialize(config *config.AppConfig) error {
	if err := initBolt(config); err != nil {
		return err
	}

	_sourceInfoDao = &SourceInfoDao{}
	_endpointInfoDao = &EndpointInfoDao{}
	_pipelineInfoDao = &PipelineInfoDao{}

	if config.IsZkUsed() {
		if err := initZookeeper(config); err != nil {
			return err
		}
		_machineDao = &ZkMachineDao{}
		_metadataDao = &ZkMetadataDao{}
		_positionDao = &ZkPositionDao{
			positions: make(map[uint64]bool),
		}
		_stateDao = &ZkStateDao{
			states: make(map[uint64]bool),
		}
	}

	if config.IsEtcdUsed() {
		if err := initEtcd(config); err != nil {
			return err
		}
		_machineDao = &EtcdMachineDao{}
		_metadataDao = &EtcdMetadataDao{}
		_positionDao = &EtcdPositionDao{}
		_stateDao = &EtcdStateDao{}
	}

	if _positionDao == nil {
		_positionDao = &BoltPositionDao{}
	}

	if _stateDao == nil {
		_stateDao = &BoltStateDao{}
	}

	return nil
}

func Close() {
	closeBolt()
	closeZookeeper()
	closeEtcd()
}

func GetSourceInfoDao() *SourceInfoDao {
	return _sourceInfoDao
}

func GetEndpointInfoDao() *EndpointInfoDao {
	return _endpointInfoDao
}

func GetPipelineInfoDao() *PipelineInfoDao {
	return _pipelineInfoDao
}

func GetPositionDao() PositionDao {
	return _positionDao
}

func GetStateDao() StateDao {
	return _stateDao
}

func GetMachineDao() MachineDao {
	return _machineDao
}

func GetMetadataDao() MetadataDao {
	return _metadataDao
}

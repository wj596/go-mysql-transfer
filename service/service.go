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

package service

import (
	"fmt"

	"github.com/juju/errors"
	"go.uber.org/atomic"

	"go-mysql-transfer/config"
	"go-mysql-transfer/dao"
	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/util/snowflake"
)

var (
	_isCluster *atomic.Bool
	_currNode  *atomic.String
	_isLeader  *atomic.Bool
	_leader    *atomic.String
)

var (
	_authService         *AuthService
	_sourceInfoService   *SourceInfoService
	_endpointInfoService *EndpointInfoService
	_positionService     *PositionService
	_stateService        *PipelineStateService
	_pipelineInfoService *PipelineInfoService
	_alarmService        *AlarmService
	_machineService      *MachineService
	_clusterService      *ClusterService
	_electionService     ElectionService

	_leaderService   *LeaderService
	_followerService *FollowerService
)

func Initialize() error {
	_isCluster = atomic.NewBool(false)
	_currNode = atomic.NewString("")
	_isLeader = atomic.NewBool(false)
	_leader = atomic.NewString("")

	_authService = &AuthService{
		sessionMap: make(map[string]*Session),
	}

	_sourceInfoService = &SourceInfoService{
		dao: dao.GetSourceInfoDao(),
	}

	_endpointInfoService = &EndpointInfoService{
		dao: dao.GetEndpointInfoDao(),
	}

	_positionService = &PositionService{
		dao: dao.GetPositionDao(),
	}

	_stateService = &PipelineStateService{
		dao:      dao.GetStateDao(),
		runtimes: make(map[uint64]*bo.PipelineRuntime),
	}

	_pipelineInfoService = &PipelineInfoService{
		dao:         dao.GetPipelineInfoDao(),
		sourceDao:   dao.GetSourceInfoDao(),
		endpointDao: dao.GetEndpointInfoDao(),
	}

	_alarmService = &AlarmService{}
	err := _alarmService.scheduleRuntimeReport()
	if nil != err {
		return err
	}

	if config.GetIns().IsCluster() { //集群
		curr := fmt.Sprintf("%s:%d", config.GetIns().GetClusterConfig().GetBindIp(), config.GetIns().GetWebPort())
		_isCluster.Store(true)
		_currNode.Store(curr)

		_machineService = &MachineService{
			dao: dao.GetMachineDao(),
		}
		if err := _machineService.initSnowflake(); err != nil {
			return err
		}

		_clusterService = &ClusterService{
			electionSignal:            make(chan bool, 1),
			electionMonitorStarted:    atomic.NewBool(false),
			electionMonitorStopSignal: make(chan struct{}, 1),
		}

		switch config.GetIns().GetClusterCoordinator() {
		case constants.ClusterCoordinatorEtcd:
			_electionService = &EtcdElectionService{
				ensured:  atomic.NewBool(false),
				selected: atomic.NewBool(false),
				leader:   atomic.NewString(""),
			}
		case constants.ClusterCoordinatorZookeeper:
			_electionService = &ZkElectionService{
				selected:         atomic.NewBool(false),
				leader:           atomic.NewString(""),
				connectingAmount: atomic.NewInt64(0),
				downgraded:       atomic.NewBool(false),
			}
		case constants.ClusterCoordinatorMySQL:
			_electionService = &MySqlElectionService{
				selected: atomic.NewBool(false),
				leader:   atomic.NewString(""),
			}
		default:
			return errors.New("请配置分布式协调器")
		}

		if err = _clusterService.startup(); err != nil {
			return err
		}
	} else { //单机
		snowflake.Initialize(1)
		if err := _pipelineInfoService.InitStartStreams(); err != nil {
			return err
		}
	}

	return nil
}

func GetAuthService() *AuthService {
	return _authService
}

func GetSourceInfoService() *SourceInfoService {
	return _sourceInfoService
}

func GetEndpointInfoService() *EndpointInfoService {
	return _endpointInfoService
}

func GetPipelineInfoService() *PipelineInfoService {
	return _pipelineInfoService
}

func GetPositionService() *PositionService {
	return _positionService
}

func GetStateService() *PipelineStateService {
	return _stateService
}

func GetClusterService() *ClusterService {
	return _clusterService
}

func GetLeaderService() *LeaderService {
	return _leaderService
}

func GetFollowerService() *FollowerService {
	return _followerService
}

func IsCluster() bool {
	return _isCluster.Load()
}

func IsLeader() bool {
	if !_isCluster.Load() {
		return false
	}
	return _isLeader.Load()
}

func IsClusterAndLeader() bool {
	if !_isCluster.Load() {
		return false
	}
	if !_isLeader.Load() {
		return false
	}
	return true
}

func GetLeader() string {
	if _currNode == nil {
		return ""
	}
	return _leader.Load()
}

func GetCurrNode() string {
	if _currNode == nil {
		return ""
	}
	return _currNode.Load()
}

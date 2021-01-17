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
package service

import (
	"go-mysql-transfer/global"
	"go-mysql-transfer/service/election"
)

var (
	_transferService *TransferService
	_electionService election.Service
	_clusterService  *ClusterService
)

func Initialize() error {
	transferService := &TransferService{
		loopStopSignal: make(chan struct{}, 1),
	}
	err := transferService.initialize()
	if err != nil {
		return err
	}
	_transferService = transferService

	if global.Cfg().IsCluster() {
		_clusterService = &ClusterService{
			electionSignal: make(chan bool, 1),
		}
		_electionService = election.NewElection(_clusterService.electionSignal)
	}

	return nil
}

func StartUp() {
	if global.Cfg().IsCluster() {
		_clusterService.boot()
	} else {
		_transferService.StartUp()
	}
}

func Close() {
	_transferService.Close()
}

func TransferServiceIns() *TransferService {
	return _transferService
}

func ClusterServiceIns() *ClusterService {
	return _clusterService
}

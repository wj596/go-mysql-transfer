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
	"log"

	"go-mysql-transfer/global"
	"go-mysql-transfer/metrics"
)

type ClusterService struct {
	electionSignal chan bool //选举信号
}

func (s *ClusterService) boot() error {
	log.Println("start master election")
	err := _electionService.Elect()
	if err != nil {
		return err
	}

	s.startElectListener()

	return nil
}

func (s *ClusterService) startElectListener() {
	go func() {
		for {
			select {
			case selected := <-s.electionSignal:
				global.SetLeaderNode(_electionService.Leader())
				global.SetLeaderFlag(selected)
				if selected {
					metrics.SetLeaderState(metrics.LeaderState)
					_transferService.StartUp()
				} else {
					metrics.SetLeaderState(metrics.FollowerState)
					_transferService.stopDump()
				}
			}
		}

	}()
}

func (s *ClusterService) Nodes() []string {
	return _electionService.Nodes()
}

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
	slog "log"

	"go.uber.org/atomic"

	"go-mysql-transfer/dao"
	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/util/log"
)

type ClusterService struct {
	electionSignal chan bool //选举信号

	electionMonitorStopSignal chan struct{}
	electionMonitorStarted    *atomic.Bool

	metadataDao dao.MetadataDao
}

func (s *ClusterService) startup() error {
	err := _electionService.Elect()
	if err != nil {
		return err
	}

	s.startElectionMonitor()
	return nil
}

func (s *ClusterService) startElectionMonitor() {
	go func() {
		log.Info("ClusterService启动选举监控")
		s.electionMonitorStarted.Store(true)
		for {
			select {
			case selected := <-s.electionSignal:
				if selected {
					_isLeader.Store(true)
					_leader.Store(GetCurrNode())
					slog.Println(fmt.Sprintf("当前节点[%s]为主节点", GetCurrNode()))

					if _followerService != nil {
						_followerService.close()
						_followerService = nil
					}

					s.metadataDao.SyncAll() //同步元数据

					_leaderService = newLeaderService()
					_leaderService.startup()
					_leaderService.sendEvent(bo.NewDispatchEvent("主节点启动"))
				} else {
					_isLeader.Store(false)
					_leader.Store(_electionService.GetLeader())
					slog.Println(fmt.Sprintf("当前节点[%s]为从节点，主节点为[%s]", GetCurrNode(), GetLeader()))

					if _leaderService != nil {
						_leaderService.close()
						_leaderService = nil
					}

					_followerService = newFollowerService()
					_followerService.startup()
				}
			case <-s.electionMonitorStopSignal:
				log.Info("ClusterService停止选举监控")
				return
			}
		}
	}()
}

func (s *ClusterService) close() {
	if s.electionMonitorStarted.Load() {
		s.electionMonitorStopSignal <- struct{}{}
		s.electionMonitorStarted.Store(false)
	}
}

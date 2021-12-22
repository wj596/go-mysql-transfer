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
	"time"

	"github.com/uber-go/atomic"

	"go-mysql-transfer/config"
	"go-mysql-transfer/dao"
	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/util/httputils"
	"go-mysql-transfer/util/log"
)

const (
	_heartbeatUrl = "http://%s/api/leaders/heartbeat?node=%s"
)

type FollowerService struct {
	heartbeatTaskStarted    *atomic.Bool
	heartbeatTaskStopSignal chan struct{}
	heartbeatUrl            string
	heartbeat               *httputils.Heartbeat
	eventQueue              chan interface{}
	eventListenerStopSignal chan struct{}
	eventListenerStarted    *atomic.Bool
	metadataDao             dao.MetadataDao
}

func newFollowerService() *FollowerService {
	url := fmt.Sprintf(_heartbeatUrl, GetLeader(), GetCurrNode())
	return &FollowerService{
		heartbeatUrl:            url,
		heartbeat:               httputils.NewHeartbeat(url, config.GetIns().GetSecretKey()),
		heartbeatTaskStarted:    atomic.NewBool(false),
		heartbeatTaskStopSignal: make(chan struct{}, 1),
		eventQueue:              make(chan interface{}, 1024),
		eventListenerStopSignal: make(chan struct{}, 1),
		eventListenerStarted:    atomic.NewBool(false),
		metadataDao:             dao.GetMetadataDao(),
	}
}

func (s *FollowerService) AcceptEvent(event interface{}) {
	s.eventQueue <- event
}

func (s *FollowerService) handleSyncEvent(event *bo.SyncEvent) {
	s.metadataDao.SyncOne(event.MetadataType, event.MetadataId)
}

func (s *FollowerService) startHeartbeatTask() {
	if s.heartbeatTaskStarted.Load() {
		return
	}
	go func() {
		log.Info("FollowerService启动心跳任务")
		s.heartbeatTaskStarted.Store(true)
		failures := atomic.NewInt64(0)
		ticker := time.NewTicker(constants.HeartbeatInterval * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				err := s.heartbeat.Do()
				if err != nil {
					log.Errorf("FollowerService心跳发送失败[%s], url[%s]", err.Error(), s.heartbeatUrl)
					failures.Add(1)
					if failures.Load() > constants.HeartbeatFailureMaximum { //可能产生网络分区
						if _stateService.existRunningRuntime() { //存在正在运行的观点
							runtimes := _stateService.getRunningRuntimes()
							for _, runtime := range runtimes {
								serv, exist := getStreamService(runtime.PipelineId.Load())
								if exist {
									streamServicePanic(serv, "无法连接主节点")
								}
							}
						}
					}
				} else {
					failures.Store(0)
				}
			case <-s.heartbeatTaskStopSignal:
				log.Info("FollowerService停止心跳发送")
				return
			}
		}
	}()
}

func (s *FollowerService) startEventsListener() {
	if s.eventListenerStarted.Load() {
		return
	}
	go func() {
		log.Info("FollowerService启动事件监听")
		s.eventListenerStarted.Store(true)
		for {
			select {
			case v := <-s.eventQueue:
				switch vv := v.(type) {
				case *bo.SyncEvent:
					s.handleSyncEvent(vv)
				}
			case <-s.eventListenerStopSignal:
				log.Info("LeaderService停止事件监听")
				return
			}
		}
	}()
}

func (s *FollowerService) startup() {
	s.startHeartbeatTask()
	s.startEventsListener()
}

func (s *FollowerService) close() {
	if s.heartbeatTaskStarted.Load() {
		s.heartbeatTaskStopSignal <- struct{}{}
		s.heartbeatTaskStarted.Store(false)
	}
	if s.eventListenerStarted.Load() {
		s.eventListenerStopSignal <- struct{}{}
		s.eventListenerStarted.Store(false)
	}
}

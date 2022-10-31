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
	"net/http"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/uber-go/atomic"

	"go-mysql-transfer/config"
	"go-mysql-transfer/dao"
	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/util/httputils"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/stringutils"
)

const (
	_heartbeatUrl = "http://%s/cluster/leaders/heartbeat?node=%s"
)

type FollowerService struct {
	heartbeatTaskStarted    *atomic.Bool
	heartbeatTaskStopSignal chan struct{}
	heartbeatLeader         string
	heartbeatRequest        *http.Request
	secretKey               string
	lock                    sync.Mutex
	eventQueue              chan interface{}
	eventListenerStopSignal chan struct{}
	eventListenerStarted    *atomic.Bool
}

func newFollowerService() *FollowerService {
	s := &FollowerService{
		secretKey:               config.GetIns().GetSecretKey(),
		heartbeatTaskStarted:    atomic.NewBool(false),
		heartbeatTaskStopSignal: make(chan struct{}, 1),
		eventQueue:              make(chan interface{}, 1024),
		eventListenerStopSignal: make(chan struct{}, 1),
		eventListenerStarted:    atomic.NewBool(false),
	}
	s.heartbeatRequest = s.createHeartbeatRequest()
	return s
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
				err := s.doHeartbeat()
				if err != nil {
					log.Errorf("FollowerService心跳发送失败[%s], url[%s]", err.Error(), s.heartbeatRequest.RequestURI)
					failures.Add(1)
					if failures.Load() > constants.HeartbeatFailureMaximum { //可能产生网络分区
						_leader.Store("")                        //情况主节点
						if _stateService.existRunningRuntime() { //存在正在运行的管道
							runtimes := _stateService.getRunningRuntimes()
							for _, runtime := range runtimes {
								serv, exist := getStreamService(runtime.PipelineId.Load())
								if exist {
									log.Warnf("停止管道[%s]，当前集群无Leader节点", runtime.PipelineName)
									streamServicePanic(serv, "无法连接Leader节点")
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

func (s *FollowerService) AcceptEvent(event interface{}) {
	s.eventQueue <- event
}

func (s *FollowerService) handleSyncEvent(event *bo.SyncEvent) {
	dao.OnSyncEvent(event)
}

func (s *FollowerService) createHeartbeatRequest() *http.Request {
	leader := GetLeader()
	url := fmt.Sprintf(_heartbeatUrl, leader, GetCurrNode())
	request, _ := http.NewRequest(http.MethodGet, url, nil)
	request.Header.Add("Content-type", "application/json;charset=UTF-8")
	request.Header.Add("Cache-Control", "no-cache")
	request.Header.Add("Connection", "Keep-Alive")
	request.Header.Add("User-Agent", "go-mysql-transfer")
	s.heartbeatLeader = leader
	s.heartbeatRequest = request
	return s.heartbeatRequest
}

func (s *FollowerService) getHeartbeatRequest() *http.Request {
	if s.heartbeatLeader == GetLeader() {
		return s.heartbeatRequest
	}

	s.lock.Lock()
	s.createHeartbeatRequest()
	s.lock.Unlock()

	return s.heartbeatRequest
}

func (s *FollowerService) doHeartbeat() error {
	if "" == GetLeader() {
		log.Warn("心跳失败，当前集群无Leader节点")
		return nil
	}

	timestamp := time.Now().UnixNano() / 1e6
	sign := httputils.Sign(timestamp, s.secretKey)

	request := s.getHeartbeatRequest()
	request.Header.Add(httputils.HeaderParamTimestamp, stringutils.ToString(timestamp))
	request.Header.Add(httputils.HeaderParamSign, sign)
	response, err := httputils.Client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return errors.Errorf("Http请求失败,状态码[%d]", response.StatusCode)
	}

	return nil
}

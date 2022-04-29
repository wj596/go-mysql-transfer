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
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"go.uber.org/atomic"
	"stathat.com/c/consistent"

	"go-mysql-transfer/config"
	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/vo"
	"go-mysql-transfer/util/httputils"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/stringutils"
)

const (
	_sendSyncEventUrl      = "http://%s/api/followers/handle-sync-event"
	_startStreamUrl        = "http://%s/api/followers/%d/start-stream"
	_stopStreamUrl         = "http://%s/api/followers/%d/stop-stream"
	_getFollowerRuntimeUrl = "http://%s/api/followers/%d/runtime"
)

type LeaderService struct {
	followers                 map[string]*bo.ClusterNode
	lock                      sync.RWMutex
	followerMonitorStopSignal chan struct{}
	followerMonitorStarted    *atomic.Bool
	distributor               *consistent.Consistent
	allocation                *bo.PipelineAllocation
	eventQueue                chan interface{}
	eventListenerStopSignal   chan struct{}
	eventListenerStarted      *atomic.Bool
}

func newLeaderService() *LeaderService {
	return &LeaderService{
		followers:                 make(map[string]*bo.ClusterNode),
		followerMonitorStopSignal: make(chan struct{}, 1),
		followerMonitorStarted:    atomic.NewBool(false),
		distributor:               consistent.New(),
		allocation:                bo.NewPipelineAllocation(),
		eventQueue:                make(chan interface{}, 1024),
		eventListenerStopSignal:   make(chan struct{}, 1),
		eventListenerStarted:      atomic.NewBool(false),
	}
}

func (s *LeaderService) HandleHeartbeat(addr string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	follower, exist := s.followers[addr]
	if !exist {
		log.Infof("LeaderService添加节点[%s]", addr)
		s.followers[addr] = &bo.ClusterNode{
			Addr:           addr,
			IsLeader:       false,
			LastActiveTime: time.Now().Unix(),
			Deadline:       false,
		}
		event := bo.NewDispatchEvent(fmt.Sprintf("添加节点[%s]", addr))
		s.sendEvent(event)
	} else {
		follower.LastActiveTime = time.Now().Unix()
		if follower.Deadline {
			log.Infof("LeaderService节点[%s]恢复上线", addr)
			follower.Deadline = false
			event := bo.NewDispatchEvent(fmt.Sprintf("节点[%s]恢复上线", addr))
			s.sendEvent(event)
		}
	}
}

func (s *LeaderService) RemoveFollower(addr string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	_, exist := s.followers[addr]
	if exist {
		delete(s.followers, addr)
	}
}

func (s *LeaderService) GetActiveFollowers() []*bo.ClusterNode {
	s.lock.Lock()
	defer s.lock.Unlock()

	var list []*bo.ClusterNode
	for _, node := range s.followers {
		if !node.Deadline {
			list = append(list, node)
		}
	}
	return list
}

func (s *LeaderService) GetNodes() []*bo.ClusterNode {
	s.lock.Lock()
	defer s.lock.Unlock()

	var list []*bo.ClusterNode
	list = append(list, &bo.ClusterNode{
		Addr:     GetCurrNode(),
		IsLeader: true,
		Deadline: false,
	})
	for _, node := range s.followers {
		list = append(list, node)
	}
	return list
}

func (s *LeaderService) GetAllocateNode(pipelineId uint64) (string, bool) {
	return s.allocation.GetNode(pipelineId)
}

func (s *LeaderService) StartStream(pipelineId uint64) error {
	pipeline, err := _pipelineInfoService.Get(pipelineId)
	if err != nil {
		log.Error(err.Error())
		return err
	}

	var node string
	s.resetDistributor()
	node, err = s.distributor.Get(stringutils.ToString(pipelineId))
	if err != nil {
		log.Error(err.Error())
		return err
	}
	log.Infof("LeaderService在节点[%s]上启动管道[%s]", node, pipeline.Name)

	if node == GetCurrNode() {
		err = _pipelineInfoService.StartStream(pipelineId)
		if err != nil {
			log.Errorf("LeaderService在节点[%s]上启动管道[%s]失败[%s]", node, pipeline.Name, err.Error())
		}
	} else {
		url := fmt.Sprintf(_startStreamUrl, node, pipelineId)
		err = httputils.Put(url, config.GetIns().GetSecretKey(), nil)
		if err != nil {
			log.Errorf("LeaderService在节点[%s]上启动管道[%s]失败[%s]", node, pipeline.Name, err.Error())
		}
	}

	s.allocation.AddPipeline(node, pipelineId)
	return err
}

func (s *LeaderService) StopStream(pipelineId uint64) error {
	node, exist := s.allocation.GetNode(pipelineId)
	if !exist {
		return nil
	}

	pipeline, err := _pipelineInfoService.Get(pipelineId)
	if err != nil {
		log.Error(err.Error())
		return err
	}

	url := fmt.Sprintf(_stopStreamUrl, node, pipelineId)
	err = httputils.Put(url, config.GetIns().GetSecretKey(), nil)
	if err != nil {
		log.Errorf("LeaderService在节点[%s]上启动管道[%s]失败[%s]", url, pipeline.Name, err.Error())
		return err
	}
	s.allocation.RemovePipeline(node, pipelineId)
	_stateService.removeRuntime(pipelineId)

	return nil
}

func (s *LeaderService) GetFollowerRuntime(pipelineId uint64, node string) (*vo.PipelineRuntimeVO, error) {
	url := fmt.Sprintf(_getFollowerRuntimeUrl, node, pipelineId)
	body, err := httputils.Get(url, httputils.SignWithKey(config.GetIns().GetSecretKey()))
	if err != nil {
		log.Errorf("LeaderService获取节点[%s]运行时状态失败[%s]", node, err.Error())
		return nil, err
	}

	var v vo.PipelineRuntimeVO
	err = json.Unmarshal(body, &v)
	if err != nil {
		log.Errorf("LeaderService获取节点[%s]运行时状态失败[%s]", node, err.Error())
		return nil, err
	}
	return &v, nil
}

func (s *LeaderService) resetDistributor() {
	nodes := make([]string, 0)
	nodes = append(nodes, GetCurrNode())
	for _, follower := range s.GetActiveFollowers() {
		nodes = append(nodes, follower.Addr)
	}
	s.distributor.Set(nodes)
}

func (s *LeaderService) sendEvent(event interface{}) {
	s.eventQueue <- event
}

func (s *LeaderService) onSyncEvent(event *bo.SyncEvent) {
	actives := s.GetActiveFollowers()
	for _, follower := range actives {
		url := fmt.Sprintf(_sendSyncEventUrl, follower.Addr)
		err := httputils.Post(url, event, httputils.SignWithKey(config.GetIns().GetSecretKey()))
		if err != nil {
			log.Errorf("数据同步事件发送失败[%s], 从节点地址[%s]", err.Error(), url)
		} else {
			log.Errorf("数据同步事件发送成功, 从节点地址[%s]", url)
		}
	}
}

func (s *LeaderService) onDispatchEvent(event *bo.DispatchEvent) {
	log.Infof("LeaderService开始节点调度, 事件：%s", event.Reason)
	nodes := make([]string, 0)
	nodes = append(nodes, GetCurrNode())
	actives := s.GetActiveFollowers()
	for _, follower := range actives {
		nodes = append(nodes, follower.Addr)
	}
	s.distributor.Set(nodes)

	infos, _ := _pipelineInfoService.SelectList(vo.NewPipelineInfoParams())
	for _, info := range infos {
		if constants.SourceInfoStatusDisable == info.Status {
			continue
		}

		state, err := _stateService.GetState(info.Id)
		if err != nil { //未启动状态
			log.Error(err.Error())
			continue
		}

		if constants.PipelineRunStatusInitial == state.Status ||
			constants.PipelineRunStatusClose == state.Status ||
			constants.PipelineRunStatusPanic == state.Status {
			continue
		}

		pipelineId := info.Id
		node, _ := s.distributor.Get(stringutils.ToString(pipelineId))

		other, exist := s.allocation.GetNode(pipelineId)
		if exist && other != node {
			if other == GetCurrNode() {
				_pipelineInfoService.StopStream(pipelineId)
				s.allocation.RemovePipeline(other, pipelineId)
			} else {
				url := fmt.Sprintf(_stopStreamUrl, node, pipelineId)
				err = httputils.Put(url, config.GetIns().GetSecretKey(), nil)
				if err == nil {
					s.allocation.RemovePipeline(other, pipelineId)
				} else {
					log.Errorf("LeaderService停止节点[%s]上的管道[%s]失败[%s]", node, info.Name, err.Error())
				}
			}
		}
		s.allocation.AddPipeline(node, pipelineId)
	}

	for _, node := range nodes {
		for _, pipelineId := range s.allocation.GetPipelines(node) {
			info, _ := _pipelineInfoService.Get(pipelineId)
			log.Infof("LeaderService在节点[%s]上启动管道[%s]", node, info.Name)
			if node == GetCurrNode() {
				err := _pipelineInfoService.StartStream(pipelineId)
				if err != nil {
					log.Errorf("LeaderService在节点[%s]上启动管道[%s]失败[%s]", node, info.Name, err.Error())
				}
			} else {
				startUrl := fmt.Sprintf(_startStreamUrl, node, pipelineId)
				err := httputils.Put(startUrl, config.GetIns().GetSecretKey(), nil)
				if err != nil {
					log.Errorf("LeaderService在节点[%s]上启动管道[%s]失败[%s]", node, info.Name, err.Error())
				}
			}

		}
	}
}

func (s *LeaderService) startFollowerMonitor() {
	if s.followerMonitorStarted.Load() {
		return
	}
	go func() {
		log.Info("LeaderService启动节点监控")
		s.followerMonitorStarted.Store(true)
		ticker := time.NewTicker(constants.HeartbeatTimeout * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				std := time.Now().Unix()
				s.lock.RLock()
				for addr, follower := range s.followers {
					diff := std - follower.LastActiveTime
					if diff > constants.HeartbeatTimeout {
						if !follower.Deadline {
							follower.Deadline = true
							log.Warnf("节点[%s]离线", addr)
							event := bo.NewDispatchEvent(fmt.Sprintf("节点[%s]离线", addr))
							s.sendEvent(event)
						}
					}
				}
				s.lock.RUnlock()
			case <-s.followerMonitorStopSignal:
				log.Info("LeaderService停止节点监控")
				return
			}
		}
	}()
}

func (s *LeaderService) startEventsListener() {
	if s.eventListenerStarted.Load() {
		return
	}
	go func() {
		log.Info("LeaderService启动事件监听")
		s.eventListenerStarted.Store(true)
		for {
			select {
			case v := <-s.eventQueue:
				switch vv := v.(type) {
				case *bo.SyncEvent:
					s.onSyncEvent(vv)
				case *bo.DispatchEvent:
					s.onDispatchEvent(vv)
				}
			case <-s.eventListenerStopSignal:
				log.Info("LeaderService停止事件监听")
				return
			}
		}
	}()
}

func (s *LeaderService) startup() {
	s.startFollowerMonitor()
	s.startEventsListener()
}

func (s *LeaderService) close() {
	if s.followerMonitorStarted.Load() {
		s.followerMonitorStopSignal <- struct{}{}
		s.followerMonitorStarted.Store(false)
	}
	if s.eventListenerStarted.Load() {
		s.eventListenerStopSignal <- struct{}{}
		s.eventListenerStarted.Store(false)
	}
}

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
package election

import (
	"fmt"
	"log"
	"sync"

	"github.com/samuel/go-zookeeper/zk"
	"go.uber.org/atomic"

	"go-mysql-transfer/global"
	"go-mysql-transfer/storage"
	"go-mysql-transfer/util/logs"
)

type zkElection struct {
	once     sync.Once
	informCh chan bool

	selected atomic.Bool
	leader   atomic.String

	connectingAmount atomic.Int64
	downgraded       atomic.Bool
}

func newZkElection(_informCh chan bool) *zkElection {
	return &zkElection{
		informCh: _informCh,
	}
}

func (s *zkElection) Elect() error {
	data := []byte(global.CurrentNode())
	acl := zk.WorldACL(zk.PermAll)
	_, err := storage.ZKConn().Create(global.Cfg().ZkElectionDir(), data, zk.FlagEphemeral, acl)
	if err == nil {
		s.beLeader()
	} else {
		v, _, err := storage.ZKConn().Get(global.Cfg().ZkElectionDir())
		if err != nil {
			return err
		}
		leader := string(v)
		if leader == global.CurrentNode() && s.downgraded.Load() {
			s.beLeader()
		} else {
			s.beFollower(leader)
		}
	}

	// register
	dir := global.Cfg().ZkNodesDir() + "/" + global.CurrentNode()
	storage.ZKConn().Create(dir, data, zk.FlagEphemeral, acl)

	s.once.Do(func() {
		s.startConnectionWatchTask()
		s.startNodeWatchTask()
	})

	return nil
}

func (s *zkElection) IsLeader() bool {
	return s.selected.Load()
}

func (s *zkElection) Leader() string {
	return s.leader.Load()
}

func (s *zkElection) Nodes() []string {
	v, _, err := storage.ZKConn().Children(global.Cfg().ZkNodesDir())
	if err != nil {
		return nil
	}
	return v
}

func (s *zkElection) beLeader() {
	s.selected.Store(true)
	s.leader.Store(global.CurrentNode())
	s.informCh <- s.selected.Load()
	log.Println("the current node is the master")
}

func (s *zkElection) beFollower(leader string) {
	s.selected.Store(false)
	s.leader.Store(leader)
	s.informCh <- s.selected.Load()
	log.Println(fmt.Sprintf("The current node is the follower, master node is : %s", leader))
}

func (s *zkElection) startConnectionWatchTask() {
	logs.Info("Start zookeeper connection Status watch task")
	go func() {
		for {
			select {
			case event := <-storage.ZKStatusSignal():
				logs.Infof("ZK ConnStatus: %v", event)
				if s.selected.Load() {
					if zk.StateConnecting == event.State {
						s.connectingAmount.Inc()
					}
					if s.connectingAmount.Load() > int64(len(storage.ZKAddresses())) {
						s.downgrading()
					}
				}
				if zk.StateHasSession == event.State {
					s.connectingAmount.Store(0)
					if s.downgraded.Load() {
						logs.Info("zookeeper HasSession restart elect")
						s.Elect()
						s.downgraded.Store(false)
					}
				}
			}
		}
	}()
}

func (s *zkElection) startNodeWatchTask() {
	go func() {

		logs.Info("Start zookeeper election node watch task")
		_, _, ch, _ := storage.ZKConn().ChildrenW(global.Cfg().ZkElectionDir())
		for {
			select {
			case childEvent := <-ch:
				if childEvent.Type == zk.EventNodeDeleted {
					logs.Info("Start elect new master ...")
					err := s.Elect()
					if err != nil {
						logs.Errorf("elect new master error %s ", err.Error())
					}
				}
			}
		}

	}()
}

func (s *zkElection) downgrading() {
	if !s.downgraded.Load() {
		log.Println("Lost contact with zookeeper, The current node degraded to Follower")
		s.downgraded.Store(true)
		s.beFollower("")
	}
}
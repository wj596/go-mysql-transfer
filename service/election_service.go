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
	"context"
	"sync"
	"time"

	"github.com/go-zookeeper/zk"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.uber.org/atomic"

	"go-mysql-transfer/dao"
	"go-mysql-transfer/util/etcdutils"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/nodepath"
)

const _electionNodeTTL = 2 //秒

type ElectionService interface {
	Elect() error
	IsLeader() bool
	GetLeader() string
}

type ZkElectionService struct {
	once             sync.Once
	selected         *atomic.Bool
	leader           *atomic.String
	connectingAmount *atomic.Int64
	downgraded       *atomic.Bool
}

type EtcdElectionService struct {
	once     sync.Once
	selected *atomic.Bool
	ensured  *atomic.Bool
	leader   *atomic.String
}

func (s *ZkElectionService) Elect() error {
	conn := dao.GetZkConn()
	path := nodepath.GetElectionNode()
	_, err := dao.GetZkConn().Create(path, []byte(GetCurrNode()), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err == nil {
		s.onLeader()
	} else {
		var v []byte
		v, _, err = conn.Get(path)
		if err != nil {
			return err
		}
		leader := string(v)
		if leader == GetCurrNode() && s.downgraded.Load() {
			s.onLeader()
		} else {
			s.onFollower(leader)
		}
	}

	s.once.Do(func() {
		s.startConnectionMonitor()
		s.startElectionNodeMonitor()
	})

	return nil
}

func (s *ZkElectionService) onLeader() {
	s.selected.Store(true)
	s.leader.Store(GetCurrNode())
	_clusterService.electionSignal <- s.selected.Load()
	log.Infof("当前节点[%s]成为主节点", GetCurrNode())
}

func (s *ZkElectionService) onFollower(leader string) {
	s.selected.Store(false)
	s.leader.Store(leader)
	_clusterService.electionSignal <- s.selected.Load()
	log.Infof("当前节点[%s]成为从节点,主节点为[%s]", GetCurrNode(), leader)
}

func (s *ZkElectionService) startConnectionMonitor() {
	go func() {
		log.Infof("启动Zk连接状态监控")
		for {
			select {
			case event := <-dao.GetZkConnSignal():
				log.Infof("Zk当前连接状态[%v]", event)
				if s.selected.Load() {
					if zk.StateConnecting == event.State {
						s.connectingAmount.Inc()
					}
					if s.connectingAmount.Load() > int64(len(dao.GetZkAddrList())) {
						s.downgrading()
					}
				}
				if zk.StateHasSession == event.State {
					s.connectingAmount.Store(0)
					if s.downgraded.Load() {
						log.Info("zookeeper HasSession restart elect")
						s.Elect()
						s.downgraded.Store(false)
					}
				}
			}
		}
	}()
}

func (s *ZkElectionService) startElectionNodeMonitor() {
	go func() {
		log.Info("启动Election节点监控")
		_, _, ch, _ := dao.GetZkConn().ChildrenW(nodepath.GetElectionNode())
		for {
			select {
			case childEvent := <-ch:
				if childEvent.Type == zk.EventNodeDeleted {
					log.Info("ElectionDir Deleted，重新选举主节点")
					err := s.Elect()
					if err != nil {
						log.Errorf("选举新主节点失败[%s]", err.Error())
					}
				}
			}
		}
	}()
}

func (s *ZkElectionService) downgrading() {
	if !s.downgraded.Load() {
		log.Infof("Zk连接丢失，当前节点降级为从节点") //当ZK集群不可用时，业务集群中不应存在主节点
		s.downgraded.Store(true)
		s.onFollower("")
	}
}

func (s *ZkElectionService) IsLeader() bool {
	return s.selected.Load()
}

func (s *ZkElectionService) GetLeader() string {
	return s.leader.Load()
}

func (s *EtcdElectionService) Elect() error {
	s.doElect()
	s.ensureFollower()
	return nil
}

func (s *EtcdElectionService) doElect() {
	go func() {
		for {
			session, err := concurrency.NewSession(dao.GetEtcdConn(), concurrency.WithTTL(_electionNodeTTL))
			if err != nil {
				log.Infof("主节点选举失败[%s]", err.Error())
				return
			}

			path := nodepath.GetElectionNode()
			election := concurrency.NewElection(session, path)
			ctx := context.Background()
			if err = election.Campaign(ctx, GetCurrNode()); err != nil {
				log.Error(err.Error())
				session.Close()
				s.onFollower("")
				continue
			}

			select {
			case <-session.Done():
				s.onFollower("")
				continue
			default:
				s.onLeader()
				err = etcdutils.UpdateOrCreate(path, election.Key(), dao.GetEtcdOps())
				if err != nil {
					log.Error(err.Error())
					return
				}
			}

			shouldBreak := false
			for !shouldBreak {
				select {
				case <-session.Done():
					log.Warn("etcd session has done")
					shouldBreak = true
					s.onFollower("")
					break
				case <-ctx.Done():
					ctxTmp, _ := context.WithTimeout(context.Background(), time.Second*_electionNodeTTL)
					election.Resign(ctxTmp)
					session.Close()
					s.onFollower("")
					return
				}
			}
		}
	}()
}

func (s *EtcdElectionService) ensureFollower() {
	go func() {
		for {
			if s.selected.Load() {
				break
			}
			path := nodepath.GetElectionNode()
			k, _, err := etcdutils.Get(path, dao.GetEtcdOps())
			if err != nil {
				log.Error(err.Error())
				continue
			}

			var leader []byte
			leader, _, err = etcdutils.Get(string(k), dao.GetEtcdOps())
			if err != nil {
				log.Error(err.Error())
				continue
			}

			s.ensured.Store(true)
			s.onFollower(string(leader))
			break
		}
	}()
}

func (s *EtcdElectionService) onLeader() {
	s.selected.Store(true)
	s.leader.Store(GetCurrNode())
	_clusterService.electionSignal <- s.selected.Load()
	log.Infof("当前节点[%s]成为主节点", GetCurrNode())
}

func (s *EtcdElectionService) onFollower(leader string) {
	s.selected.Store(false)
	_clusterService.electionSignal <- s.selected.Load()
	s.leader.Store(leader)
	log.Infof("当前节点[%s]成为从节点,主节点为[%s]", GetCurrNode(), leader)
}

func (s *EtcdElectionService) IsLeader() bool {
	return s.selected.Load()
}

func (s *EtcdElectionService) GetLeader() string {
	return s.leader.Load()
}

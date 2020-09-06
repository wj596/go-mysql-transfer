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
package cluster

import (
	"context"
	"sync"
	"time"

	"github.com/juju/errors"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.uber.org/atomic"

	"go-mysql-transfer/global"
	"go-mysql-transfer/storage"
	"go-mysql-transfer/util/etcdutil"
	"go-mysql-transfer/util/logutil"
)

const _electionNodeTTL = 5

type etcdElection struct {
	once sync.Once

	cfg      *global.Config
	informCh chan bool

	flag   atomic.Bool
	leader atomic.String
}

func newEtcdElection(_informCh chan bool, _cfg *global.Config) *etcdElection {
	return &etcdElection{
		informCh: _informCh,
		cfg:      _cfg,
	}
}

func (s *etcdElection) Elect() error {
	go s.doElect()

	go func() {
		for {
			if s.flag.Load() {
				break
			}
			err := s.ensureLeader()
			if err == nil {
				s.informFalse()
				break
			}
		}
		logutil.BothInfof("End master election")
	}()

	return nil
}

func (s *etcdElection) doElect() error {
	for {
		session, err := concurrency.NewSession(storage.EtcdConn(), concurrency.WithTTL(_electionNodeTTL))
		if err != nil {
			return errors.Trace(err)
		}

		election := concurrency.NewElection(session, s.cfg.ZeElectionDir())
		ctx := context.Background()

		if err = election.Campaign(ctx, s.cfg.Cluster.CurrentNode); err != nil {
			logutil.Error(errors.ErrorStack(err))
			session.Close()
			s.informFalse()
			continue
		}

		logutil.BothInfof("elected key : %s", election.Key())
		s.elected(election.Key(), s.cfg)
		s.informTrue()

		shouldBreak := false
		for !shouldBreak {
			select {
			case <-session.Done():
				logutil.Warn("etcd session has done")
				shouldBreak = true
				s.informFalse()
				break
			case <-ctx.Done():
				ctxTmp, _ := context.WithTimeout(context.Background(), time.Second*_electionNodeTTL)
				election.Resign(ctxTmp)
				session.Close()
				s.informFalse()
				return nil
			}
		}
	}
}

func (s *etcdElection) IsLeader() bool {
	return s.flag.Load()
}

func (s *etcdElection) Leader() string {
	if s.flag.Load() {
		return s.cfg.Cluster.CurrentNode
	}
	return s.leader.Load()
}

func (s *etcdElection) ensureLeader() error {
	elected, _, err := etcdutil.Get(s.cfg.ZeElectedDir(), storage.EtcdOps())
	if err != nil {
		return err
	}

	data, _, err := etcdutil.Get(string(elected), storage.EtcdOps())
	if err != nil {
		return err
	}

	current := string(data)
	s.leader.Store(current)

	return nil
}

func (s *etcdElection) informTrue() {
	s.flag.Store(true)
	s.informCh <- s.flag.Load()
}

func (s *etcdElection) informFalse() {
	s.flag.Store(false)
	s.informCh <- s.flag.Load()
}

func (s *etcdElection) elected(key string, cfg *global.Config) error {
	err := etcdutil.UpdateOrCreate(cfg.ZeElectedDir(), key, storage.EtcdOps())
	if err != nil {
		logutil.Error(errors.ErrorStack(err))
		return err
	}

	return nil
}

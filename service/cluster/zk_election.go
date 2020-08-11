package cluster

import (
	"sync"

	"github.com/samuel/go-zookeeper/zk"
	"go.uber.org/atomic"

	"go-mysql-transfer/global"
	"go-mysql-transfer/storage"
	"go-mysql-transfer/util/logutil"
)

type zkElection struct {
	once sync.Once

	cfg      *global.Config
	informCh chan bool

	flag   atomic.Bool
	leader atomic.String
}

func newZkElection(_informCh chan bool, _cfg *global.Config) *zkElection {
	return &zkElection{
		informCh: _informCh,
		cfg:      _cfg,
	}
}

func (s *zkElection) Elect() error {
	data := []byte(s.cfg.Cluster.CurrentNode)

	acl := zk.WorldACL(zk.PermAll)
	_, err := storage.ZKConn().Create(s.cfg.ZeElectionDir(), data, zk.FlagEphemeral, acl)
	if err == nil {
		s.flag.Store(true)
	} else {
		s.flag.Store(false)
		v, _, err := storage.ZKConn().Get(s.cfg.ZeElectionDir())
		if err != nil {
			return err
		}
		s.leader.Store(string(v))
	}

	s.inform()

	s.once.Do(func() {
		go s.startWatchTask()
	})

	return nil
}

func (s *zkElection) IsLeader() bool {
	return s.flag.Load()
}

func (s *zkElection) Leader() string {
	if s.flag.Load() {
		return s.cfg.Cluster.CurrentNode
	}
	return s.leader.Load()
}

func (s *zkElection) inform() {
	s.informCh <- s.flag.Load()
}

func (s *zkElection) startWatchTask() {
	logutil.Info("Start Zookeeper watch task")
	_, _, ch, _ := storage.ZKConn().ChildrenW(s.cfg.ZeElectionDir())
	for {
		select {
		case childEvent := <-ch:
			if childEvent.Type == zk.EventNodeDeleted {
				logutil.Info("Start elect new master ...")
				err := s.Elect()
				if err != nil {
					logutil.Errorf("elect new master error %s ", err.Error())
				}
			}
		}
	}
}

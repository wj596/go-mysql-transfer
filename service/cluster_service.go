package service

import (
	"go-mysql-transfer/global"
	"go-mysql-transfer/service/cluster"
	"go-mysql-transfer/util/logutil"
)

type ClusterService struct {
	electionCh chan bool
	election   cluster.Election
}

func (s *ClusterService) boot(cfg *global.Config) error {
	s.electionCh = make(chan bool, 1)
	s.election = cluster.NewElection(s.electionCh, cfg)
	logutil.BothInfof("Start master election...")

	err := s.election.Elect()
	if err != nil {
		return err
	}

	go s.startElectListener()

	return nil
}

func (s *ClusterService) startElectListener() {
	for {
		select {
		case flag := <-s.electionCh:
			if flag {
				global.SetLeaderState(global.MetricsStateOK)
				TransferServiceIns().Restart()
				logutil.BothInfof("The current node is the master")
			} else {
				global.SetLeaderState(global.MetricsStateNO)
				TransferServiceIns().Pause()
				logutil.BothInfof("The current node is the follower, master node is : %s", s.election.Leader())
			}
		}
	}
}

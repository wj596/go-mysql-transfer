package cluster

import (
	"go-mysql-transfer/global"
)

type Election interface {
	Elect() error
	IsLeader() bool
	Leader() string
}

func NewElection(_informCh chan bool, cfg *global.Config) Election {
	if cfg.IsZk() {
		return newZkElection(_informCh, cfg)
	}
	if cfg.IsEtcd() {
		return newEtcdElection(_informCh, cfg)
	}

	return nil
}

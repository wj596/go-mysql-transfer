package storage

import "go-mysql-transfer/global"

type ElectionStorage interface {
	Elect() error
}

func NewElectionStorage(conf *global.Config) PositionStorage {
	if conf.IsCluster() {
		if conf.IsZk() {
			return &zkPositionStorage{
				Conf: conf,
			}
		}
		if conf.IsEtcd() {

		}
	}

	return nil
}


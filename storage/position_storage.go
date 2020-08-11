package storage

import (
	"github.com/siddontang/go-mysql/mysql"

	"go-mysql-transfer/global"
)

type PositionStorage interface {
	Initialize() error
	Save(pos mysql.Position) error
	Get() (mysql.Position, error)
}

func NewPositionStorage(conf *global.Config) PositionStorage {
	if conf.IsCluster() {
		if conf.IsZk() {
			return &zkPositionStorage{
				Conf: conf,
			}
		}
		if conf.IsEtcd() {
			return &etcdPositionStorage{
				Conf: conf,
			}
		}
	}

	return &boltPositionStorage{}
}

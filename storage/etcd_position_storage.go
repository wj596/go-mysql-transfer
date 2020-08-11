package storage

import (
	"encoding/json"

	"github.com/siddontang/go-mysql/mysql"

	"go-mysql-transfer/global"
	"go-mysql-transfer/util/etcdutil"
)

type etcdPositionStorage struct {
	Conf *global.Config
}

func (s *etcdPositionStorage) Initialize() error {
	data, err := json.Marshal(mysql.Position{})
	if err != nil {
		return err
	}

	err = etcdutil.CreateIfNecessary(s.Conf.ZePositionDir(), string(data), _etcdOps)
	if err != nil {
		return err
	}

	return nil
}

func (s *etcdPositionStorage) Save(pos mysql.Position) error {
	data, err := json.Marshal(pos)
	if err != nil {
		return err
	}

	return etcdutil.Save(s.Conf.ZePositionDir(), string(data), _etcdOps)
}

func (s *etcdPositionStorage) Get() (mysql.Position, error) {
	var entity mysql.Position

	data, _, err := etcdutil.Get(s.Conf.ZePositionDir(), _etcdOps)
	if err != nil {
		return entity, err
	}

	err = json.Unmarshal(data, &entity)

	return entity, err
}

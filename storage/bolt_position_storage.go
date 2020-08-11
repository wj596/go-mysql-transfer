package storage

import (
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/vmihailenco/msgpack"
	"go.etcd.io/bbolt"
)

type boltPositionStorage struct {
}

func (s *boltPositionStorage) Initialize() error {
	return _bolt.Update(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_positionBucket)
		data := bt.Get(_fixPositionId)
		if data != nil {
			return nil
		}

		bytes, err := msgpack.Marshal(mysql.Position{})
		if err != nil {
			return err
		}
		return bt.Put(_fixPositionId, bytes)
	})
}

func (s *boltPositionStorage) Save(pos mysql.Position) error {
	return _bolt.Update(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_positionBucket)
		data, err := msgpack.Marshal(pos)
		if err != nil {
			return err
		}
		return bt.Put(_fixPositionId, data)
	})
}

func (s *boltPositionStorage) Get() (mysql.Position, error) {
	var entity mysql.Position
	err := _bolt.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_positionBucket)
		data := bt.Get(_fixPositionId)
		if data == nil {
			return errors.NotFoundf("PositionStorage")
		}

		return msgpack.Unmarshal(data, &entity)
	})

	return entity, err
}

package storage

import (
	"github.com/juju/errors"
	"go.etcd.io/bbolt"

	"go-mysql-transfer/util/byteutil"
)

type BoltRowStorage struct {
}

func (s *BoltRowStorage) Size() int {
	var size int
	_bolt.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_rowRequestBucket)
		size = bt.Stats().KeyN
		return nil
	})

	return size
}

func (s *BoltRowStorage) Add(data []byte) {
	_bolt.Update(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_rowRequestBucket)
		seq, _ := bt.NextSequence()
		return bt.Put(byteutil.Uint64ToBytes(seq), data)
	})
}

func (s *BoltRowStorage) BatchAdd(list [][]byte) {
	_bolt.Batch(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_rowRequestBucket)
		for _, data := range list {
			seq, _ := bt.NextSequence()
			bt.Put(byteutil.Uint64ToBytes(seq), data)
		}
		return nil
	})
}

func (s *BoltRowStorage) IdList() ([][]byte, error) {
	ls := make([][]byte, 0)
	err := _bolt.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_rowRequestBucket)
		cursor := bt.Cursor()
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			ls = append(ls, k)
		}
		return nil
	})

	return ls, err
}

func (s *BoltRowStorage) Get(key []byte) ([]byte, error) {
	var entity []byte
	err := _bolt.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_rowRequestBucket)
		data := bt.Get(key)
		if data == nil {
			return errors.NotFoundf("Row")
		}

		entity = data
		return nil
	})

	if err != nil {
		return nil, err
	}

	return entity, nil
}

func (s *BoltRowStorage) Delete(key []byte) error {
	return _bolt.Update(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_rowRequestBucket)
		return bt.Delete(key)
	})
}

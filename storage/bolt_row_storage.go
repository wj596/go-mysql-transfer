package storage

import (
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

func (s *BoltRowStorage) List() (map[uint64][]byte, error) {
	ret := make(map[uint64][]byte)
	err := _bolt.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_rowRequestBucket)
		cursor := bt.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			ret[byteutil.BytesToUint64(k)] = v
		}
		return nil
	})

	return ret, err
}

func (s *BoltRowStorage) Delete(key uint64) error {
	return _bolt.Update(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_rowRequestBucket)
		return bt.Delete(byteutil.Uint64ToBytes(key))
	})
}

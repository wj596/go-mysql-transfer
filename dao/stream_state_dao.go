package dao

import (
	"encoding/json"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"
	"go.etcd.io/bbolt"

	"go-mysql-transfer/domain/po"
)

type StreamStateDao struct {
}

func (s *StreamStateDao) SavePosition(pipelineId uint64, pos mysql.Position) error {
	return _mdb.Update(func(tx *bbolt.Tx) error {
		data, err := json.Marshal(&pos)
		if err != nil {
			return err
		}
		return tx.Bucket(_positionBucket).Put(marshalId(pipelineId), data)
	})
}

func (s *StreamStateDao) GetPosition(pipelineId uint64) mysql.Position {
	var entity mysql.Position
	_mdb.View(func(tx *bbolt.Tx) error {
		data := tx.Bucket(_positionBucket).Get(marshalId(pipelineId))
		if data == nil {
			return errors.NotFoundf("Position")
		}
		return json.Unmarshal(data, &entity)
	})
	return entity
}

func (s *StreamStateDao) SaveStreamState(pipelineId uint64, state po.StreamState) error {
	return _mdb.Update(func(tx *bbolt.Tx) error {
		data, err := json.Marshal(&state)
		if err != nil {
			return err
		}
		return tx.Bucket(_streamStateBucket).Put(marshalId(pipelineId), data)
	})
}

func (s *StreamStateDao) GetStreamState(pipelineId uint64) po.StreamState {
	var entity po.StreamState
	_mdb.View(func(tx *bbolt.Tx) error {
		data := tx.Bucket(_streamStateBucket).Get(marshalId(pipelineId))
		if data == nil {
			return errors.NotFoundf("StreamState")
		}
		return json.Unmarshal(data, &entity)
	})
	return entity
}

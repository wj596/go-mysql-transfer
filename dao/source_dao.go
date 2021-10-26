package dao

import (
	"encoding/json"
	"strings"

	"github.com/juju/errors"
	"go.etcd.io/bbolt"

	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/domain/vo"
	"go-mysql-transfer/util/log"
)

type SourceInfoDao struct {
}

func (s *SourceInfoDao) getBucket(tx *bbolt.Tx) *bbolt.Bucket {
	return tx.Bucket(_sourceBucket)
}

func (s *SourceInfoDao) Save(entity *po.SourceInfo) error {
	return _mdb.Update(func(tx *bbolt.Tx) error {
		data, err := json.Marshal(entity)
		if err != nil {
			return err
		}
		return s.getBucket(tx).Put(marshalId(entity.Id), data)
	})
}

func (s *SourceInfoDao) Delete(id uint64) error {
	return _mdb.Update(func(tx *bbolt.Tx) error {
		return s.getBucket(tx).Delete(marshalId(id))
	})
}

func (s *SourceInfoDao) Get(id uint64) (*po.SourceInfo, error) {
	var entity po.SourceInfo
	err := _mdb.View(func(tx *bbolt.Tx) error {
		data := s.getBucket(tx).Get(marshalId(id))
		if data == nil {
			return errors.NotFoundf("SourceInfo")
		}
		return json.Unmarshal(data, &entity)
	})

	if nil != err {
		return nil, err
	}
	return &entity, err
}

func (s *SourceInfoDao) GetByName(name string) (*po.SourceInfo, error) {
	var entity po.SourceInfo
	var found bool
	err := _mdb.View(func(tx *bbolt.Tx) error {
		cursor := s.getBucket(tx).Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			if err := json.Unmarshal(v, &entity); err == nil {
				if name == entity.Name {
					found = true
					break
				}
			}
		}
		return nil
	})

	if err != nil {
		log.Error(err.Error())
		return nil, err
	}
	if !found {
		return nil, errors.NotFoundf("SourceInfo")
	}
	return &entity, err
}

func (s *SourceInfoDao) SelectList(params *vo.SourceInfoParams) ([]*po.SourceInfo, error) {
	list := make([]*po.SourceInfo, 0)
	err := _mdb.View(func(tx *bbolt.Tx) error {
		cursor := s.getBucket(tx).Cursor()
		for k, v := cursor.Last(); k != nil; k, v = cursor.Prev() {
			var entity po.SourceInfo
			if err := json.Unmarshal(v, &entity); err == nil {
				if params.Name != "" && !strings.Contains(entity.Name, params.Name) {
					continue
				}
				if params.Host != "" && !strings.Contains(entity.Host, params.Host) {
					continue
				}
				list = append(list, &entity)
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return list, err
}

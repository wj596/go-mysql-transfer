package dao

import (
	"encoding/json"
	"github.com/juju/errors"
	"go.etcd.io/bbolt"
	//"google.golang.org/protobuf/proto"

	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/domain/vo"
)

type TransformRuleDao struct {
}

func (s *TransformRuleDao) getBucket(tx *bbolt.Tx) *bbolt.Bucket {
	return tx.Bucket(_ruleBucket)
}

func (s *TransformRuleDao) Get(id uint64) (*po.TransformRule, error) {
	var entity po.TransformRule
	err := _mdb.View(func(tx *bbolt.Tx) error {
		data := s.getBucket(tx).Get(marshalId(id))
		if data == nil {
			return errors.NotFoundf("TransformRule")
		}
		return json.Unmarshal(data, &entity)
	})

	if nil != err {
		return nil, err
	}
	return &entity, err
}

func (s *TransformRuleDao) SelectList(params vo.TransformRuleParams) ([]*po.TransformRule, error) {
	list := make([]*po.TransformRule, 0)
	err := _mdb.View(func(tx *bbolt.Tx) error {
		cursor := s.getBucket(tx).Cursor()
		for k, v := cursor.Last(); k != nil; k, v = cursor.Prev() {
			var entity po.TransformRule
			if err := json.Unmarshal(v, &entity); err == nil {
				if params.PipelineId != 0 && entity.PipelineInfoId != params.PipelineId {
					continue
				}
				if params.EndpointType != 0 && entity.EndpointType != params.EndpointType {
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
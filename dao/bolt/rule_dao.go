package bolt

import (
	"github.com/juju/errors"
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"

	"go-mysql-transfer/model/po"
	"go-mysql-transfer/util/byteutil"
)

type TransformRuleDaoImpl struct {
}

func (s *TransformRuleDaoImpl) Get(id uint64) (*po.TransformRule, error){
	var entity po.TransformRule
	err := _conn.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_ruleBucket)
		data := bt.Get(byteutil.Uint64ToBytes(id))
		if data == nil {
			return errors.NotFoundf("TransformRule")
		}
		return proto.Unmarshal(data, &entity)
	})

	return &entity, err
}

func (s *TransformRuleDaoImpl) SelectList(pipelineId uint64, endpointType int32) ([]*po.TransformRule, error) {
	list := make([]*po.TransformRule, 0)
	err := _conn.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_ruleBucket)
		cursor := bt.Cursor()
		for k, v := cursor.Last(); k != nil; k, v = cursor.Prev() {
			var entity po.TransformRule
			if err := proto.Unmarshal(v, &entity); err == nil {
				if pipelineId != 0 && entity.PipelineInfoId == pipelineId {
					list = append(list, &entity)
				}
				if endpointType != 0 && entity.EndpointType == endpointType {
					list = append(list, &entity)
				}
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return list, err
}

package bolt

import (
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"

	"go-mysql-transfer/model/po"
)

type TransformRuleDaoImpl struct {
}

func (s *TransformRuleDaoImpl) SelectList(pipelineId uint64) ([]*po.TransformRule, error) {
	list := make([]*po.TransformRule, 0)
	err := _conn.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_ruleBucket)
		cursor := bt.Cursor()
		for k, v := cursor.Last(); k != nil; k, v = cursor.Prev() {
			var entity po.TransformRule
			if err := proto.Unmarshal(v, &entity); err == nil {
				if entity.PipelineInfoId == pipelineId {
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

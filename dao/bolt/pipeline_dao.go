package bolt

import (
	"strings"

	"github.com/juju/errors"
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"

	"go-mysql-transfer/model/po"
	"go-mysql-transfer/model/vo"
	"go-mysql-transfer/util/byteutil"
	"go-mysql-transfer/util/log"
)

type PipelineInfoDaoImpl struct {
}

func (s *PipelineInfoDaoImpl) Insert(pipeline *po.PipelineInfo, rules []*po.TransformRule) error {
	return _conn.Update(func(tx *bbolt.Tx) error {
		pBt := tx.Bucket(_pipelineBucket)
		rBt := tx.Bucket(_ruleBucket)

		pData, err := proto.Marshal(pipeline)
		if err != nil {
			return err
		}
		if pBt.Put(byteutil.Uint64ToBytes(pipeline.Id), pData); err != nil {
			return err
		}

		for _, rule := range rules {
			rData, err := proto.Marshal(rule)
			if err != nil {
				return err
			}
			if rBt.Put(byteutil.Uint64ToBytes(rule.Id), rData); err != nil {
				return err
			}
		}

		return nil
	})
}

func (s *PipelineInfoDaoImpl) Update(pipeline *po.PipelineInfo, rules []*po.TransformRule) error {
	return _conn.Update(func(tx *bbolt.Tx) error {
		pBt := tx.Bucket(_pipelineBucket)
		rBt := tx.Bucket(_ruleBucket)

		rIds := make([]uint64, 0)
		rCursor := rBt.Cursor()
		for k, v := rCursor.First(); k != nil; k, v = rCursor.Next() {
			var temp po.TransformRule
			if err := proto.Unmarshal(v, &temp); err == nil {
				if temp.PipelineInfoId == pipeline.Id {
					rIds = append(rIds, temp.Id)
				}
			}
		}

		for _, id := range rIds {
			if err := rBt.Delete(byteutil.Uint64ToBytes(id)); err != nil {
				return err
			}
		}

		for _, rule := range rules {
			rData, err := proto.Marshal(rule)
			if err != nil {
				return err
			}
			if rBt.Put(byteutil.Uint64ToBytes(rule.Id), rData); err != nil {
				return err
			}
		}

		pData, err := proto.Marshal(pipeline)
		if err != nil {
			return err
		}
		if pBt.Put(byteutil.Uint64ToBytes(pipeline.Id), pData); err != nil {
			return err
		}

		return nil
	})
}

func (s *PipelineInfoDaoImpl) Delete(id uint64) error {
	return _conn.Update(func(tx *bbolt.Tx) error {
		pBt := tx.Bucket(_pipelineBucket)
		rBt := tx.Bucket(_ruleBucket)

		rIds := make([]uint64, 0)
		rCursor := rBt.Cursor()
		for k, v := rCursor.First(); k != nil; k, v = rCursor.Next() {
			var temp po.TransformRule
			if err := proto.Unmarshal(v, &temp); err == nil {
				if temp.PipelineInfoId == id {
					rIds = append(rIds, temp.Id)
				}
			}
		}
		for _, rid := range rIds {
			if err := rBt.Delete(byteutil.Uint64ToBytes(rid)); err != nil {
				return err
			}
		}

		return pBt.Delete(byteutil.Uint64ToBytes(id))
	})
}

func (s *PipelineInfoDaoImpl) Get(id uint64) (*po.PipelineInfo, error) {
	var entity po.PipelineInfo
	err := _conn.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_pipelineBucket)
		data := bt.Get(byteutil.Uint64ToBytes(id))
		if data == nil {
			return errors.NotFoundf("PipelineInfo")
		}
		return proto.Unmarshal(data, &entity)
	})
	return &entity, err
}

func (s *PipelineInfoDaoImpl) GetByName(name string) (*po.PipelineInfo, error) {
	var entity po.PipelineInfo
	var found bool
	err := _conn.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_pipelineBucket)
		cursor := bt.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			if err := proto.Unmarshal(v, &entity); err == nil {
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
		log.Warnf("PipelineInfo not found by name[%s]", name)
		return nil, errors.NotFoundf("PipelineInfo")
	}

	return &entity, err
}

func (s *PipelineInfoDaoImpl) SelectList(name string) ([]*vo.PipelineInfoVO, error) {
	list := make([]*vo.PipelineInfoVO, 0)
	err := _conn.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_pipelineBucket)
		cursor := bt.Cursor()
		for k, v := cursor.Last(); k != nil; k, v = cursor.Prev() {
			var temp po.PipelineInfo
			entity := new(vo.PipelineInfoVO)
			if err := proto.Unmarshal(v, &temp); err == nil {
				if name != "" && !strings.Contains(temp.Name, name) {
					continue
				}
				entity.FromPO(&temp)
				list = append(list, entity)
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return list, err
}

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

type SourceInfoDaoImpl struct {
}

func (s *SourceInfoDaoImpl) Save(entity *po.SourceInfo) error {
	return _conn.Update(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_sourceBucket)
		data, err := proto.Marshal(entity)
		if err != nil {
			return err
		}
		id := byteutil.Uint64ToBytes(entity.Id)
		return bt.Put(id, data)
	})
}

func (s *SourceInfoDaoImpl) Delete(id uint64) error {
	return _conn.Update(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_sourceBucket)
		return bt.Delete(byteutil.Uint64ToBytes(id))
	})
}

func (s *SourceInfoDaoImpl) Get(id uint64) (*po.SourceInfo, error) {
	var entity po.SourceInfo
	err := _conn.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_sourceBucket)
		data := bt.Get(byteutil.Uint64ToBytes(id))
		if data == nil {
			return errors.NotFoundf("SourceInfo")
		}
		return proto.Unmarshal(data, &entity)
	})

	return &entity, err
}

func (s *SourceInfoDaoImpl) GetByName(name string) (*po.SourceInfo, error) {
	var entity po.SourceInfo
	var found bool
	err := _conn.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_sourceBucket)
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
		log.Warnf("SourceInfo not found by name[%s]", name)
		return nil, errors.NotFoundf("SourceInfo")
	}

	return &entity, err
}

func (s *SourceInfoDaoImpl) SelectList(term *vo.SourceInfoParams) ([]*po.SourceInfo, error) {
	list := make([]*po.SourceInfo, 0)
	err := _conn.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_sourceBucket)
		cursor := bt.Cursor()
		for k, v := cursor.Last(); k != nil; k, v = cursor.Prev() {
			var entity po.SourceInfo
			if err := proto.Unmarshal(v, &entity); err == nil {
				if term.Name != "" && !strings.Contains(entity.Name, term.Name) {
					continue
				}
				if term.Host != "" && !strings.Contains(entity.Host, term.Host) {
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

func (s *SourceInfoDaoImpl) SelectPage(term *vo.SourceInfoParams) (*vo.SourceInfoResp, error) {
	list := make([]*po.SourceInfo, 0)
	err := _conn.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_sourceBucket)
		cursor := bt.Cursor()
		for k, v := cursor.Last(); k != nil; k, v = cursor.Prev() {
			var entity po.SourceInfo
			if err := proto.Unmarshal(v, &entity); err == nil {
				if term.Name != "" && !strings.Contains(entity.Name, term.Name) {
					continue
				}
				if term.Host != "" && !strings.Contains(entity.Host, term.Host) {
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

	resp := new(vo.SourceInfoResp)
	resp.SetTotal(len(list))
	if len(list) < term.Page().Limit() {
		resp.SetItems(list)
		return resp, nil
	}
	if len(list)-term.Page().StartIndex() <= term.Page().Limit() {
		resp.SetItems(list[term.Page().StartIndex():])
		return resp, err
	}
	resp.SetItems(list[term.Page().StartIndex() : term.Page().StartIndex()+term.Page().Limit()])

	return resp, err
}

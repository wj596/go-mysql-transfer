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

type EndpointInfoDaoImpl struct {
}

func (s *EndpointInfoDaoImpl) Save(entity *po.EndpointInfo) error {
	return _conn.Update(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_endpointBucket)
		data, err := proto.Marshal(entity)
		if err != nil {
			return err
		}
		id := byteutil.Uint64ToBytes(entity.Id)
		return bt.Put(id, data)
	})
}

func (s *EndpointInfoDaoImpl) Delete(id uint64) error {
	return _conn.Update(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_endpointBucket)
		return bt.Delete(byteutil.Uint64ToBytes(id))
	})
}

func (s *EndpointInfoDaoImpl) Get(id uint64) (*po.EndpointInfo, error) {
	var entity po.EndpointInfo
	err := _conn.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_endpointBucket)
		data := bt.Get(byteutil.Uint64ToBytes(id))
		if data == nil {
			return errors.NotFoundf("EndpointInfo")
		}
		return proto.Unmarshal(data, &entity)
	})

	return &entity, err
}

func (s *EndpointInfoDaoImpl) GetByName(name string) (*po.EndpointInfo, error) {
	var entity po.EndpointInfo
	var found bool
	err := _conn.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_endpointBucket)
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
		log.Errorf(err.Error())
		return nil, err
	}
	if !found {
		log.Warnf("EndpointInfo not found by name[%s]", name)
		return nil, errors.NotFoundf("EndpointInfo")
	}

	return &entity, err
}

func (s *EndpointInfoDaoImpl) SelectList(term *vo.EndpointInfoParams) ([]*po.EndpointInfo, error) {
	list := make([]*po.EndpointInfo, 0)
	err := _conn.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_endpointBucket)
		cursor := bt.Cursor()
		for k, v := cursor.Last(); k != nil; k, v = cursor.Prev() {
			var entity po.EndpointInfo
			if err := proto.Unmarshal(v, &entity); err == nil {
				if term.Name != "" && !strings.Contains(entity.Name, term.Name) {
					continue
				}
				if term.Host != "" && !strings.Contains(entity.Addresses, term.Host) {
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

func (s *EndpointInfoDaoImpl) SelectPage(term *vo.EndpointInfoParams) (*vo.EndpointInfoResp, error) {
	list := make([]*po.EndpointInfo, 0)
	err := _conn.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_endpointBucket)
		cursor := bt.Cursor()
		for k, v := cursor.Last(); k != nil; k, v = cursor.Prev() {
			var entity po.EndpointInfo
			if err := proto.Unmarshal(v, &entity); err == nil {
				if term.Name != "" && !strings.Contains(entity.Name, term.Name) {
					continue
				}
				if term.Host != "" && !strings.Contains(entity.Addresses, term.Host) {
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

	resp := vo.NewEndpointInfoResp()
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

package service

import (
	"fmt"

	"go-mysql-transfer/dao"
	"go-mysql-transfer/model/po"
	"go-mysql-transfer/util/snowflake"
)

type EndpointInfoService struct {
	dao dao.EndpointInfoDao
}

func (s *EndpointInfoService) Insert(entity *po.EndpointInfo) error {
	entity.Id, _ = snowflake.NextId()
	return s.dao.Save(entity)
}

func (s *EndpointInfoService) Update(entity *po.EndpointInfo) error {
	fmt.Println(entity.GetId())
	return s.dao.Save(entity)
}

func (s *EndpointInfoService) Delete(id uint64) error {
	return s.dao.Delete(id)
}

func (s *EndpointInfoService) Get(id uint64) (*po.EndpointInfo, error) {
	return s.dao.Get(id)
}

func (s *EndpointInfoService) GetByName(name string) (*po.EndpointInfo, error) {
	return s.dao.GetByName(name)
}

func (s *EndpointInfoService) SelectList(name string, host string) ([]*po.EndpointInfo, error) {
	return s.dao.SelectList(name, host)
}

func (s *EndpointInfoService) TestLink(vo *po.EndpointInfo) error {
	return nil //mysql.TestConnection(vo.GetUsername(), vo.GetPassword(), vo.GetHost(), vo.GetPort())
}
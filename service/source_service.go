package service

import (
	"fmt"

	"go-mysql-transfer/dao"
	"go-mysql-transfer/datasource"
	"go-mysql-transfer/model/po"
	"go-mysql-transfer/model/vo"
	"go-mysql-transfer/util/snowflake"
)

type SourceInfoService struct {
	dao dao.SourceInfoDao
}

func (s *SourceInfoService) Insert(entity *po.SourceInfo) error {
	entity.Id, _ = snowflake.NextId()
	return s.dao.Save(entity)
}

func (s *SourceInfoService) Update(entity *po.SourceInfo) error {
	fmt.Println(entity.GetId())
	return s.dao.Save(entity)
}

func (s *SourceInfoService) Delete(id uint64) error {
	return s.dao.Delete(id)
}

func (s *SourceInfoService) Get(id uint64) (*po.SourceInfo, error) {
	return s.dao.Get(id)
}

func (s *SourceInfoService) GetByName(name string) (*po.SourceInfo, error) {
	return s.dao.GetByName(name)
}

func (s *SourceInfoService) SelectList(name string, host string) ([]*po.SourceInfo, error) {
	return s.dao.SelectList(name, host)
}

func (s *SourceInfoService) SelectSchemaList(id uint64) ([]string, error) {
	ds, err := s.dao.Get(id)
	if err != nil {
		return nil, err
	}
	cc, err := datasource.CreateCanal(ds)
	if err != nil {
		return nil, err
	}
	defer datasource.CloseCanal(cc)

	ls, err := datasource.SelectSchemaNameList(cc)
	if err != nil {
		return nil, err
	}

	return ls, nil
}

func (s *SourceInfoService) SelectTableList(id uint64, schemaName string) ([]string, error) {
	ds, err := s.dao.Get(id)
	if err != nil {
		return nil, err
	}
	cc, err := datasource.CreateCanal(ds)
	if err != nil {
		return nil, err
	}
	defer datasource.CloseCanal(cc)

	ls, err := datasource.SelectTableNameList(cc, schemaName)
	if err != nil {
		return nil, err
	}

	return ls, nil
}

func (s *SourceInfoService) SelectTableInfo(id uint64, schemaName, tableName string) (*vo.TableInfo, error) {
	ds, err := s.dao.Get(id)
	if err != nil {
		return nil, err
	}
	cc, err := datasource.CreateCanal(ds)
	if err != nil {
		return nil, err
	}
	defer datasource.CloseCanal(cc)

	result, err := datasource.SelectTableInfo(cc, schemaName, tableName)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (s *SourceInfoService) TestLink(vo *po.SourceInfo) error {
	return nil
	//return mysql.TestConnection(vo.GetUsername(), vo.GetPassword(), vo.GetHost(), vo.GetPort())
}

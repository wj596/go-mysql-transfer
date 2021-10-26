package dao

import (
	"fmt"
	"testing"

	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/domain/vo"
)

func TestSaveEndpointInfo(t *testing.T) {
	before(t)

	entity := &po.EndpointInfo{
		Id:        1,
		Name:      "reids测试2",
		Type:      constants.EndpointTypeRedis,
		Addresses: "127.0.0.1:6379",
		Username:  "",
		Password:  "",
		Status:    0,
		// Redis
		DeployType: 0,
		Database:   0,
	}

	GetEndpointInfoDao().Save(entity)
}

func TestGetEndpointInfo(t *testing.T) {
	before(t)

	entity, err := GetEndpointInfoDao().Get(1)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(entity)
}

func TestGetEndpointInfoByName(t *testing.T) {
	before(t)

	entity, err := GetSourceInfoDao().GetByName("办公自动化数据库")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(entity)
}

func TestEndpointInfoList(t *testing.T) {
	before(t)

	params := new(vo.SourceInfoParams)
	params.Name = "自动化"
	params.Host = "192"
	entity, err := GetSourceInfoDao().SelectList(params)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(entity)
}

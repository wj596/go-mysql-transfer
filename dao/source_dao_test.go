package dao

import (
	"fmt"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/domain/vo"
	"testing"
)

func TestSaveSourceInfo(t *testing.T) {
	before(t)

	entity := &po.SourceInfo{
		Id:       1,
		Name:     "办公自动化数据库",
		Host:     "192.115.123.117",
		Port:     3306,
		Username: "root",
		Password: "123456",
		Charset:  "utf8",
		SlaveID:  5,
		Flavor:   "mysql",
	}

	GetSourceInfoDao().Save(entity)
}

func TestGetSourceInfo(t *testing.T) {
	before(t)

	entity,err :=GetSourceInfoDao().Get(1)
	if err!=nil{
		t.Fatal(err)
	}
	fmt.Println(entity)
}

func TestGetSourceInfoByName(t *testing.T) {
	before(t)

	entity,err :=GetSourceInfoDao().GetByName("办公自动化数据库")
	if err!=nil{
		t.Fatal(err)
	}
	fmt.Println(entity)
}

func TestSourceInfoList(t *testing.T) {
	before(t)

	params := new(vo.SourceInfoParams)
	params.Name = "自动化"
	params.Host = "192"
	entity,err :=GetSourceInfoDao().SelectList(params)
	if err!=nil{
		t.Fatal(err)
	}
	fmt.Println(entity)
}

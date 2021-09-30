package bolt

import (
	"fmt"

	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/util/snowflake"
	"go-mysql-transfer/util/stringutils"
	"testing"
)

func TestSourceInfoDao_Save(t *testing.T) {
	before(t)

	entity := &po.SourceInfo{
		Id:       1,
		Name:     "办公自动化数据库",
		Host:     "192.115.123.117",
		Port:     3306,
		Username: "root",
		Password: "123456",
		Charset:  "UTF-8",
		SlaveID:  5,
		Flavor:   "mysql",
	}

	dao := new(SourceInfoDao)

	err := dao.Save(entity)
	if err != nil {
		t.Fatal(err.Error())
	}
}

func TestInitData(t *testing.T) {
	before(t)
	dao := new(SourceInfoDao)

	for i := 0; i < 25; i++ {
		id, _ := snowflake.NextId()
		entity := &po.SourceInfo{
			Id:       id,
			Name:     "办公自动化数据库" + stringutils.ToString(i),
			Host:     "192.115.123." + stringutils.ToString(i),
			Port:     3306,
			Username: "root",
			Password: "123456",
			Charset:  "UTF-8",
			SlaveID:  5,
			Flavor:   "mysql",
			Status:   uint32(i % 2),
		}
		err := dao.Save(entity)
		if err != nil {
			t.Fatal(err.Error())
		}
	}
}

func TestSourceInfoDao_Get(t *testing.T) {
	before(t)

	dao := new(SourceInfoDao)

	entity, err := dao.Get(368950241963016193)
	if err != nil {
		t.Fatal(err.Error())
	}
	fmt.Println(stringutils.ToJsonIndent(entity))
}

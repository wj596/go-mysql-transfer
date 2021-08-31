package bolt

import (
	"fmt"
	"github.com/juju/errors"
	"go-mysql-transfer/config"
	"go-mysql-transfer/model/po"
	"go-mysql-transfer/util/snowflake"
	"go-mysql-transfer/util/stringutils"
	"testing"
)

func before(t *testing.T) {
	configFile := "D:\\newtransfers\\application.yml"
	if err := config.Initialize(configFile); err != nil {
		println(errors.ErrorStack(err))
		return
	}

	err := Initialize(config.GetIns())
	if err != nil {
		t.Fatal(err.Error())
	}

	snowflake.Initialize(1)
}

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

	dao := new(SourceInfoDaoImpl)

	err := dao.Save(entity)
	if err != nil {
		t.Fatal(err.Error())
	}
}

func TestInitData(t *testing.T) {
	before(t)
	dao := new(SourceInfoDaoImpl)

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

	dao := new(SourceInfoDaoImpl)

	entity, err := dao.Get(368950241963016193)
	if err != nil {
		t.Fatal(err.Error())
	}
	fmt.Println(stringutils.ToJsonIndent(entity))
}

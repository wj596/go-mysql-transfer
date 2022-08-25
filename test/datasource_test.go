package test

import (
	"go-mysql-transfer/datasource"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/util/jsonutils"
	"testing"
)

func TestConnection(t *testing.T) {
	ds := &po.SourceInfo{
		Host: "10.1.71.200",
		Port: 3306,
		Username: "root",
		Password: "jqkj123$%^",
		Flavor: "mysql",
	}
	err := datasource.TestConnect(ds)
	if err!=nil {
		println(err.Error())
	}
	ls, err := datasource.FilterTableNameList(ds, "zhmz-respool","^res_*")
	if err!=nil {
		println(err.Error())
	}
	println(jsonutils.ToJsonIndent(ls))
}

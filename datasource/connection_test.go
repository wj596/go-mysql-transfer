package datasource

import (
	"fmt"
	"testing"

	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/po"
)

func TestCreateCanal(t *testing.T) {
	ds := &po.SourceInfo{
		Host:     "192.168.44.113", //主机
		Port:     3306,             //端口
		Username: "root",           //用户名
		Password: "root",           //密码
		Charset:  "utf8",           //字符串编码
		Flavor:   "mysql",          // mysql or mariadb,默认mysql
	}
	cc, err := createCanal(ds)
	if err != nil {
		t.Fatal(err.Error())
	}

	cc.Close()
}

func TestSelectSchemaNameList(t *testing.T) {
	ds := &po.SourceInfo{
		Host:     "192.168.44.113", //主机
		Port:     3306,             //端口
		Username: "root",           //用户名
		Password: "root",           //密码
		Charset:  "utf8",           //字符串编码
		Flavor:   "mysql",          // mysql or mariadb,默认mysql
	}
	cc, err := createCanal(ds)
	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Println(SelectSchemaNameList(ds))

	cc.Close()
}

func TestSelectTableNameList(t *testing.T) {
	ds := &po.SourceInfo{
		Host:     "192.168.44.113", //主机
		Port:     3306,             //端口
		Username: "root",           //用户名
		Password: "root",           //密码
		Charset:  "utf8",           //字符串编码
		Flavor:   "mysql",          // mysql or mariadb,默认mysql
	}
	cc, err := createCanal(ds)
	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Println(SelectTableNameList(ds, "baseapis"))

	cc.Close()
}

func TestSelectTableInfo(t *testing.T) {
	ds := &po.SourceInfo{
		Host:     "127.0.0.1", //主机
		Port:     3306,        //端口
		Username: "root",      //用户名
		Password: "123456",    //密码
		Charset:  "utf8",      //字符串编码
		Flavor:   "mysql",     // mysql or mariadb,默认mysql
	}
	cc, err := createCanal(ds)
	if err != nil {
		t.Fatal(err.Error())
	}

	var tt *bo.TableInfo
	tt, err = SelectTableInfo(ds, "zhmz-basic", "t_role")
	if err != nil {
		t.Fatal(err.Error())
	}
	fmt.Println(tt)
	cc.Close()
}

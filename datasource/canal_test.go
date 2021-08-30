package datasource

import (
	"fmt"
	"go-mysql-transfer/model/vo"
	"testing"

	"go-mysql-transfer/model/po"
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
	cc, err := CreateCanal(ds)
	if err != nil {
		t.Fatal(err.Error())
	}

	CloseCanal(cc)
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
	cc, err := CreateCanal(ds)
	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Println(SelectSchemaNameList(cc))

	CloseCanal(cc)
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
	cc, err := CreateCanal(ds)
	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Println(SelectTableNameList(cc, "baseapis"))

	CloseCanal(cc)
}

func TestSelectTableInfo(t *testing.T) {
	ds := &po.SourceInfo{
		Host:     "127.0.0.1", //主机
		Port:     3306,             //端口
		Username: "root",           //用户名
		Password: "123456",           //密码
		Charset:  "utf8",           //字符串编码
		Flavor:   "mysql",          // mysql or mariadb,默认mysql
	}
	cc, err := CreateCanal(ds)
	if err != nil {
		t.Fatal(err.Error())
	}

	var tt *vo.TableInfo
	tt, err = SelectTableInfo(cc, "zhmz-basic", "t_role")
	if err != nil {
		t.Fatal(err.Error())
	}
	fmt.Println(tt)
	CloseCanal(cc)
}

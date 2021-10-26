package datasource

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/util/stringutils"
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
	cc, err := CreateConnection(ds)
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
	cc, err := CreateConnection(ds)
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
	cc, err := CreateConnection(ds)
	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Println(SelectTableNameList(ds, "baseapis"))

	cc.Close()
}

func TestSelectTableInfo(t *testing.T) {
	ds := &po.SourceInfo{
		Host:     "192.168.44.119", //主机
		Port:     3306,             //端口
		Username: "root2",          //用户名
		Password: "123456",         //密码
		Charset:  "utf8",           //字符串编码
		Flavor:   "mysql",          // mysql or mariadb,默认mysql
	}
	cc, err := CreateConnection(ds)
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

func TestCanalSelectList(t *testing.T) {
	ds := &po.SourceInfo{
		Host:     "127.0.0.1", //主机
		Port:     3306,        //端口
		Username: "root",      //用户名
		Password: "123456",    //密码
		Charset:  "utf8",      //字符串编码
		Flavor:   "mysql",     // mysql or mariadb,默认mysql
	}
	cc, err := CreateConnection(ds)
	if err != nil {
		t.Fatal(err.Error())
	}

	resultSet, err := cc.Execute("SELECT * FROM eseap.memberinfo limit 1")
	if err != nil {
		t.Fatal(err)
	}
	rowNumber := resultSet.RowNumber()
	for i := 0; i < rowNumber; i++ {
		rowValues := make([]interface{}, 0, 16)
		for j := 0; j < 16; j++ {
			val, err := resultSet.GetValue(i, j)
			if err != nil {
				break
			}
			rowValues = append(rowValues, val)
		}
		fmt.Println(rowValues)
	}
}

func TestConnSelectList(t *testing.T) {
	ds := &po.SourceInfo{
		Host:     "127.0.0.1", //主机
		Port:     3306,        //端口
		Username: "root",      //用户名
		Password: "123456",    //密码
		Charset:  "utf8",      //字符串编码
		Flavor:   "mysql",     // mysql or mariadb,默认mysql
	}

	scheme := "eseap"
	elements := make([]string, 0)
	elements = append(elements, ds.GetUsername(), ":", ds.GetPassword(), "@tcp(", ds.GetHost(), ":", stringutils.ToString(ds.GetPort()), ")/")
	elements = append(elements, scheme)
	elements = append(elements, "?timeout=5s")
	if ds.GetCharset() != "" {
		charset := "&charset=" + ds.GetCharset()
		elements = append(elements, charset)
	}
	dataSourceName := strings.Join(elements, "")
	conn, err := sql.Open(ds.GetFlavor(), dataSourceName)
	if err != nil {
		t.Fatal(err.Error())
	}

	rows, err := conn.Query("SELECT * FROM eseap.memberinfo limit 1")
	if err != nil {
		t.Fatal(err)
	}
	scans := make([]interface{}, 16)
	for index, _ := range scans { //为每一列初始化一个指针
		scans[index] = new(interface{})
	}
	for rows.Next() {
		rows.Scan(scans...)
	}
	for i := range scans {
		scans[i] = *(scans[i].(*interface{}))
	}
	fmt.Println(scans)

}

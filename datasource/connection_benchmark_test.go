package datasource

import (
	"database/sql"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/util/stringutils"
	"strings"
	"testing"
)

func BenchmarkCanalParallel(b *testing.B) {
	ds := &po.SourceInfo{
		Host:     "127.0.0.1", //主机
		Port:     3306,        //端口
		Username: "root",      //用户名
		Password: "123456",    //密码
		Charset:  "utf8",      //字符串编码
		Flavor:   "mysql",     // mysql or mariadb,默认mysql
	}
	pool, err := NewConnectionPool(3,ds)
	if err != nil {
		b.Fatal(err.Error())
	}

	for jj := 0; jj < b.N; jj++ {
		resultSet, err := pool.Get().Execute("SELECT * FROM eseap.memberinfo limit 1")
		if err != nil {
			b.Fatal(err)
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
			//fmt.Println(rowValues)
		}
	}
	pool.Shutdown()
}

func BenchmarkCanal(b *testing.B) {
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
		b.Fatal(err.Error())
	}

	for jj := 0; jj < b.N; jj++ {
		resultSet, err := cc.Execute("SELECT * FROM eseap.memberinfo limit 1")
		if err != nil {
			b.Fatal(err)
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
			//fmt.Println(rowValues)
		}
	}
	cc.Close()
}

func BenchmarkSql(b *testing.B) {
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
		b.Fatal(err.Error())
	}

	for jj := 0; jj < b.N; jj++ {
		rows, err := conn.Query("SELECT * FROM eseap.memberinfo limit 1")
		if err != nil {
			b.Fatal(err)
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
		//fmt.Println(scans)
	}
	conn.Close()
}
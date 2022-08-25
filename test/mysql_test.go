package test

import (
	"testing"

	"github.com/go-xorm/xorm"
)

type Metadata struct {
	Id uint64 `json:"id,string" xorm:"pk"`
	Data []byte
}

func TestXorm(t *testing.T) {
	en, err := xorm.NewEngine("mysql", "root:123456@tcp(127.0.0.1:3306)/go_mysql_transfer?parseTime=true")
	if err != nil {
		println("connect to database error")
		panic(err)
	}
	println("connect success")
	err = en.Ping()
	if nil != err {
		println(err.Error())
	}

	//sql := "INSERT INTO t_source_metadata(`id`, `data`) VALUES (?, ?)"



	//result, err :=en.Exec(sql, 3, []byte(sql))
	//if err != nil {
	//	println(err.Error())
	//}
	//println(result.LastInsertId())

	var metadata Metadata
	_, err = en.SQL("select * from t_source_metadata where id = 3").Get(&metadata)
	if err != nil {
		println(err.Error())
	}
	println(metadata.Id)
	println(string(metadata.Data))

}

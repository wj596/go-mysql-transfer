package model

import (
	"encoding/json"
	"fmt"
	"github.com/vmihailenco/msgpack"
	"go-mysql-transfer/model/po"
	"go-mysql-transfer/util/stringutils"
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestPageRequest(t *testing.T) {

	bo := &po.SourceInfo{
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

	data, _ := json.Marshal(bo)
	fmt.Println(len(data))

	data2, _ := msgpack.Marshal(bo)
	fmt.Println(len(data2))

	data3, _ := proto.Marshal(bo)
	fmt.Println(len(data3))
	var entity po.SourceInfo
	proto.Unmarshal(data3, &entity)
	fmt.Println(stringutils.ToJsonString(entity))
}

func BenchmarkJson(b *testing.B) {
	bo := &po.SourceInfo{
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

	var entity po.SourceInfo

	for i := 100000; i > 0; i-- {
		data, _ := json.Marshal(bo)
		json.Unmarshal(data, &entity)
	}

}

func BenchmarkMsgpack(b *testing.B) {
	bo := &po.SourceInfo{
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

	var entity po.SourceInfo

	for i := 100000; i > 0; i-- {
		data, _ := msgpack.Marshal(bo)
		msgpack.Unmarshal(data, &entity)
	}

}

func BenchmarkProto(b *testing.B) {
	bo := &po.SourceInfo{
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

	var entity po.SourceInfo

	for i := 100000; i > 0; i-- {
		data, _ := proto.Marshal(bo)
		proto.Unmarshal(data, &entity)
	}

}

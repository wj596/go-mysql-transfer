package zkutils

import (
	"github.com/go-zookeeper/zk"
	"log"
	"testing"
	"time"
)

type ZkLoggerAgent2 struct {
}

func NewZkLoggerAgent2() *ZkLoggerAgent2 {
	return &ZkLoggerAgent2{}
}

func (s *ZkLoggerAgent2) Printf(template string, args ...interface{}) {

}

func BenchmarkZkExists(b *testing.B) {
	option := zk.WithLogger(NewZkLoggerAgent2())
	conn, _, err := zk.Connect([]string{"127.0.0.1:2181"}, time.Second, option) //*10)
	if err != nil {
		log.Fatal(err.Error())
	}
	//e,s,r :=conn.Exists("/go-mysql-transfer/metadata/source/379835330724888577")
	//fmt.Println(e)
	//fmt.Println(s)
	//fmt.Println(r)

	for i := 0; i < b.N; i++ {
		conn.Exists("/go-mysql-transfer/metadata/source/379835330724888577")
	}

}

func BenchmarkZkGet(b *testing.B) {
	option := zk.WithLogger(NewZkLoggerAgent2())
	conn, _, err := zk.Connect([]string{"127.0.0.1:2181"}, time.Second, option) //*10)
	if err != nil {
		log.Fatal(err.Error())
	}
	for i := 0; i < b.N; i++ {
		conn.Get("/go-mysql-transfer/metadata/source/379835330724888577")
	}
}

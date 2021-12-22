package zkutils

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/pingcap/log"

	"go-mysql-transfer/util/logagent"
)

func TestCreateNodeIfNecessary(t *testing.T) {
	option := zk.WithLogger(logagent.NewZkLoggerAgent())
	conn, _, err := zk.Connect([]string{"127.0.0.1:2181"}, time.Second, option) //*10)
	if err != nil {
		log.Fatal(err.Error())
	}

	err = CreateNodeIfNecessary("/test", conn)
	if err != nil {
		log.Fatal(err.Error())
	}
}

func TestCreateNodeWithDataIfNecessary(t *testing.T) {
	option := zk.WithLogger(logagent.NewZkLoggerAgent())
	conn, _, err := zk.Connect([]string{"127.0.0.1:2181"}, time.Second, option) //*10)
	if err != nil {
		log.Fatal(err.Error())
	}

	err = CreateNodeWithDataIfNecessary("/test2", []byte("test"), conn)
	if err != nil {
		log.Fatal(err.Error())
	}
}

func TestCreateNodeWithFlagIfNecessary(t *testing.T) {
	option := zk.WithLogger(logagent.NewZkLoggerAgent())
	conn, _, err := zk.Connect([]string{"127.0.0.1:2181"}, time.Second, option) //*10)
	if err != nil {
		log.Fatal(err.Error())
	}
	CreateNodeIfNecessary("/seq", conn)
	CreateNodeWithFlagIfNecessary("/seq/test3", zk.FlagSequence, conn)
	CreateNodeWithFlagIfNecessary("/seq/test4", zk.FlagSequence, conn)
	CreateNodeWithFlagIfNecessary("/seq/test5", zk.FlagSequence, conn)
}

func TestSetNode(t *testing.T) {
	option := zk.WithLogger(logagent.NewZkLoggerAgent())
	conn, _, err := zk.Connect([]string{"127.0.0.1:2181"}, time.Second, option) //*10)
	if err != nil {
		log.Fatal(err.Error())
	}

	conn.Set("/test", []byte("sssss"), -1)

	b, s, e := conn.Get("/test")
	fmt.Println(b)
	fmt.Println(s.DataLength)
	fmt.Println(e)
	fmt.Println("--------")
	b2, s2, e2 := conn.Exists("/test")
	fmt.Println(b2)
	fmt.Println(s2.DataLength)
	fmt.Println(e2)

}

func TestDeleteNode(t *testing.T) {
	option := zk.WithLogger(logagent.NewZkLoggerAgent())
	conn, _, err := zk.Connect([]string{"127.0.0.1:2181"}, time.Second, option) //*10)
	if err != nil {
		log.Fatal(err.Error())
	}

	err = DeleteNode("/test2", conn)
	if err != nil {
		log.Fatal(err.Error())
	}
}

func TestGetNode(t *testing.T) {
	option := zk.WithLogger(logagent.NewZkLoggerAgent())
	conn, _, err := zk.Connect([]string{"127.0.0.1:2181"}, time.Second, option) //*10)
	if err != nil {
		log.Fatal(err.Error())
	}

	data, s, err := conn.Get("/test2")
	if err != nil {
		fmt.Println("sss", err)
		//log.Fatal(err.Error())
	}
	fmt.Println(data)
	fmt.Println(s)
}

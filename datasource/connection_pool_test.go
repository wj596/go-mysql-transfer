package datasource

import (
	"fmt"
	"go-mysql-transfer/domain/po"
	"testing"
	"time"
)

func TestConnectionPool(t *testing.T) {
	ds := &po.SourceInfo{
		Host:     "192.168.44.113", //主机
		Port:     3306,             //端口
		Username: "root",           //用户名
		Password: "root",           //密码
		Charset:  "utf8",           //字符串编码
		Flavor:   "mysql",          // mysql or mariadb,默认mysql
	}

	pool, err := NewConnectionPool(3, ds)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		go func() {
			for i := 0; i < 10; i++ {
				fmt.Println(pool.Get())
			}
		}()
	}

	time.Sleep(30 * time.Second)
	pool.Shutdown()
}

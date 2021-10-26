package datasource

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/siddontang/go-mysql/canal"

	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/stringutils"
)

func TestConnect(ds *po.SourceInfo) error {
	scheme := "mysql"
	elements := make([]string, 0)
	elements = append(elements, ds.GetUsername(), ":", ds.GetPassword(), "@tcp(", ds.GetHost(), ":", stringutils.ToString(ds.GetPort()), ")/")
	elements = append(elements, scheme)
	elements = append(elements, "?timeout=5s")
	if ds.GetCharset() != "" {
		charset := "&charset=" + ds.GetCharset()
		elements = append(elements, charset)
	}
	dataSourceName := strings.Join(elements, "")
	db, err := sql.Open(ds.GetFlavor(), dataSourceName)
	if err != nil {
		return err
	}
	defer db.Close()

	log.Infof("测试数据库连接：user[%s]、host[%s]、port[%d]", ds.GetUsername(), ds.GetHost(), ds.GetPort())

	return db.Ping()
}

func CreateConnection(ds *po.SourceInfo) (*canal.Canal, error) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", ds.GetHost(), ds.GetPort())
	cfg.User = ds.GetUsername()
	cfg.Password = ds.GetPassword()
	cfg.Flavor = ds.GetFlavor()
	if ds.GetCharset() != "" {
		cfg.Charset = ds.GetCharset()
	}
	//if ds.GetSlaveID() != 0 {
		//cfg.ServerID = ds.GetSlaveID()
	//}
	cfg.Dump.DiscardErr = false
	cfg.Dump.ExecutionPath = ""

	canal, err := canal.NewCanal(cfg)
	if err != nil {
		return nil, err
	}

	return canal, nil
}

func CloseConnection(canal *canal.Canal) {
	if canal != nil {
		canal.Close()
	}
}

package datasource

import (
	"database/sql"
	"strings"

	_ "github.com/go-sql-driver/mysql"

	"go-mysql-transfer/model/po"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/stringutils"
)

func CreateConnection(ds *po.SourceInfo, scheme string) (*sql.DB, error) {
	log.Infof("build MySQL connection,user[%s]、host[%s]、port[%d]", ds.GetUsername(), ds.GetHost(), ds.GetPort())
	elements := make([]string, 0)
	elements = append(elements, ds.GetUsername(), ":", ds.GetPassword(), "@tcp(", ds.GetHost(), ":", stringutils.ToString(ds.GetPort()), ")/")
	elements = append(elements, scheme)
	if ds.GetCharset() != "" {
		elements = append(elements, "?charset=utf8")
	}
	db, err := sql.Open(ds.GetFlavor(), strings.Join(elements, ""))
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		if db != nil {
			db.Close()
		}
		return nil, err
	}

	return db, nil
}

func CloseConnection(conn *sql.DB) error {
	if conn != nil {
		return conn.Close()
	}
	return nil
}

func TestConnection(ds *po.SourceInfo, scheme string) error {
	conn, err := CreateConnection(ds, scheme)
	defer CloseConnection(conn)
	return err
}

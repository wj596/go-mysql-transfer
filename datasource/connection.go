package datasource

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"

	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/stringutils"
)

func CreateConnection(ds *po.SourceInfo, scheme string) (*sql.DB, error) {
	log.Infof("build MySQL connection,user[%s]、host[%s]、port[%d]", ds.GetUsername(), ds.GetHost(), ds.GetPort())
	elements := make([]string, 0)
	elements = append(elements, ds.GetUsername(), ":", ds.GetPassword(), "@tcp(", ds.GetHost(), ":", stringutils.ToString(ds.GetPort()), ")/")
	elements = append(elements, scheme)
	elements = append(elements, "?timeout=5s")
	if ds.GetCharset() != "" {
		charset := "&charset=" + ds.GetCharset()
		elements = append(elements, charset)
	}

	dataSourceName := strings.Join(elements, "")
	fmt.Println(dataSourceName)
	db, err := sql.Open(ds.GetFlavor(), dataSourceName)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		CloseConnection(db)
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

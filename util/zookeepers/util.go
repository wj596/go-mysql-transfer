package zookeepers

import (
	"strings"

	"github.com/samuel/go-zookeeper/zk"

	"go-mysql-transfer/util/stringutil"
)

func CreateDirIfNecessary(dir string, conn *zk.Conn) error {
	exist, _, err := conn.Exists(dir)
	if err != nil {
		return err
	}
	if !exist {
		_, err := conn.Create(dir, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return err
		}
	}

	return nil
}

func CreateDirWithDataIfNecessary(dir string,data []byte, conn *zk.Conn) error {
	exist, _, err := conn.Exists(dir)
	if err != nil {
		return err
	}
	if !exist {
		_, err := conn.Create(dir, data, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return err
		}
	}

	return nil
}

func DeleteDir(dir string, conn *zk.Conn) error {
	_, stat, err := conn.Get(dir)
	if err != nil {
		return err
	}

	return conn.Delete(dir, stat.Version)
}

func JoinDir(args ...interface{}) string {
	path := ""
	for _, arg := range args {
		if arg != "" {
			p := stringutil.ToString(arg)
			if strings.HasPrefix(p, "/") {
				path = path + p
			} else {
				path = path + "/" + p
			}
		}
	}
	return path
}

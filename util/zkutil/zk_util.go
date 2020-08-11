package zkutil

import (
	"github.com/samuel/go-zookeeper/zk"
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

func DeleteDir(dir string, conn *zk.Conn) error {
	_, stat, err := conn.Get(dir)
	if err != nil {
		return err
	}

	return conn.Delete(dir, stat.Version)
}

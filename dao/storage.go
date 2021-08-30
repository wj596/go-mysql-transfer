package dao

import (
	"go-mysql-transfer/config"
	"go-mysql-transfer/dao/bolt"
	"go-mysql-transfer/dao/etcd"
	"go-mysql-transfer/dao/zk"
)

func Initialize(config *config.AppConfig) error {
	if err := bolt.Initialize(config); err != nil {
		return err
	}

	if err := zk.Initialize(config); err != nil {
		return err
	}

	if err := etcd.Initialize(config); err != nil {
		return err
	}

	return nil
}

func Close() {
	bolt.Close()
	zk.Close()
	etcd.Close()
}

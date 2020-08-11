package storage

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"go.etcd.io/bbolt"
	"go.etcd.io/etcd/clientv3"
	etcdlog "go.etcd.io/etcd/pkg/logutil"

	"go-mysql-transfer/global"
	"go-mysql-transfer/util/byteutil"
	"go-mysql-transfer/util/fileutil"
	"go-mysql-transfer/util/logutil"
	"go-mysql-transfer/util/zkutil"
)

const (
	_boltFilePath = "db"
	_boltFileName = "data.db"
	_boltFileMode = 0600
)

var (
	_rowRequestBucket = []byte("RowRequest")
	_positionBucket   = []byte("Position")
	_fixPositionId    = byteutil.Uint64ToBytes(uint64(1))

	_bolt     *bbolt.DB
	_zkConn   *zk.Conn
	_etcdConn *clientv3.Client
	_etcdOps  clientv3.KV
)

func InitStorage(conf *global.Config) error {
	if err := initBolt(conf); err != nil {
		return err
	}

	if conf.IsZk() {
		if err := initZk(conf); err != nil {
			return err
		}
	}

	if conf.IsEtcd() {
		if err := initEtcd(conf); err != nil {
			return err
		}
	}

	return nil
}

func initBolt(conf *global.Config) error {
	blotStorePath := filepath.Join(conf.DataDir, _boltFilePath)
	if err := fileutil.MkdirIfNecessary(blotStorePath); err != nil {
		return errors.New(fmt.Sprintf("create boltdb store : %s", err.Error()))
	}

	boltFilePath := filepath.Join(blotStorePath, _boltFileName)
	bolt, err := bbolt.Open(boltFilePath, _boltFileMode, bbolt.DefaultOptions)
	if err != nil {
		return errors.New(fmt.Sprintf("open boltdb: %s", err.Error()))
	}

	err = bolt.Update(func(tx *bbolt.Tx) error {
		tx.CreateBucketIfNotExists(_rowRequestBucket)
		tx.CreateBucketIfNotExists(_positionBucket)
		return nil
	})

	_bolt = bolt

	return err
}

func initZk(conf *global.Config) error {
	option := zk.WithLogger(logutil.NewZkLoggerAgent())
	list := strings.Split(conf.Cluster.ZkAddrs, ",")
	conn, _, err := zk.Connect(list, time.Second, option) //*10)

	if err != nil {
		return err
	}

	if conf.Cluster.ZkAuthentication != "" {
		err = conn.AddAuth("digest", []byte(conf.Cluster.ZkAuthentication))
		if err != nil {
			return err
		}
	}

	err = zkutil.CreateDirIfNecessary(conf.ZeRootDir(), conn)
	if err != nil {
		return err
	}

	err = zkutil.CreateDirIfNecessary(conf.ZeClusterDir(), conn)
	if err != nil {
		return err
	}

	_zkConn = conn

	return nil
}

func initEtcd(conf *global.Config) error {
	etcdlog.DefaultZapLoggerConfig = logutil.EtcdZapLoggerConfig()
	clientv3.SetLogger(logutil.NewEtcdLoggerAgent())

	list := strings.Split(conf.Cluster.EtcdAddrs, ",")
	config := clientv3.Config{
		Endpoints:   list,
		Username:    conf.Cluster.EtcdUser,
		Password:    conf.Cluster.EtcdPassword,
		DialTimeout: 10 * time.Second,
	}

	client, err := clientv3.New(config)
	if err != nil {
		return err
	}
	_etcdConn = client
	_etcdOps = clientv3.NewKV(_etcdConn)

	return nil
}

func ZKConn() *zk.Conn {
	return _zkConn
}

func EtcdConn() *clientv3.Client {
	return _etcdConn
}

func EtcdOps() clientv3.KV {
	return _etcdOps
}

func CloseStorage() {
	if _bolt != nil {
		_bolt.Close()
	}
	if _zkConn != nil {
		_zkConn.Close()
	}
	if _etcdConn != nil {
		_etcdConn.Close()
	}
}

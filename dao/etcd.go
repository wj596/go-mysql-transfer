package dao

import (
	"strings"
	"time"

	"go.etcd.io/etcd/clientv3"

	"go-mysql-transfer/config"
	"go-mysql-transfer/util/logagent"
)

var (
	_etcdConn      *clientv3.Client
	_etcdOps       clientv3.KV
	_etcdAddresses []string
)

func initEtcd(app *config.AppConfig) error {
	clientv3.SetLogger(logagent.NewEtcdLoggerAgent())
	addresses := strings.Split(app.GetClusterConfig().GetEtcdAddrs(), ",")
	conn, err := clientv3.New(clientv3.Config{
		Endpoints:   addresses,
		Username:    app.GetClusterConfig().GetEtcdUser(),
		Password:    app.GetClusterConfig().GetEtcdPassword(),
		DialTimeout: 1 * time.Second,
	})
	if err != nil {
		return err
	}

	//err = etcdutils.CreateNodeIfNecessary(conf.PositionDir(""), string(data), _ops)
	//if err != nil {
	//	return err
	//}

	_etcdConn = conn
	_etcdOps = clientv3.NewKV(conn)
	_etcdAddresses = addresses

	return nil
}

func closeEtcd() {
	if _etcdConn != nil {
		_etcdConn.Close()
	}
}

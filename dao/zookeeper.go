package dao

import (
	"strings"
	"time"

	"github.com/go-zookeeper/zk"

	"go-mysql-transfer/config"
	"go-mysql-transfer/util/logagent"
	"go-mysql-transfer/util/zkutils"
)

var (
	_zkConn       *zk.Conn
	_zkConnSignal <-chan zk.Event
	_zkAddresses  []string
)

func initZookeeper(config *config.AppConfig) error {
	option := zk.WithLogger(logagent.NewZkLoggerAgent())
	addresses := strings.Split(config.GetClusterConfig().GetZkAddrs(), ",")
	conn, signal, err := zk.Connect(addresses, time.Second, option) //*10)
	if err != nil {
		return err
	}

	if config.GetClusterConfig().GetZkAuthentication() != "" {
		err = conn.AddAuth("digest", []byte(config.GetClusterConfig().GetZkAuthentication()))
		if err != nil {
			return err
		}
	}

	// 初始化Root节点
	if err := zkutils.CreateNodeIfNecessary(getRootNode(), conn); err != nil {
		return err
	}

	_zkConn = conn
	_zkConnSignal = signal
	_zkAddresses = addresses

	return nil
}

func closeZookeeper() {
	if _zkConn != nil {
		_zkConn.Close()
	}
}

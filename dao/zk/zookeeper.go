package zk

import (
	"strings"
	"time"

	"github.com/go-zookeeper/zk"

	"go-mysql-transfer/config"
	"go-mysql-transfer/dao/path"
	"go-mysql-transfer/util/logagent"
	"go-mysql-transfer/util/zkutils"
)

var (
	_zkConn       *zk.Conn
	_zkConnSignal <-chan zk.Event
	_zkAddresses  []string
)

func Initialize(app *config.AppConfig) error {
	if !app.IsZkUsed() {
		return nil
	}

	option := zk.WithLogger(logagent.NewZkLoggerAgent())
	addresses := strings.Split(app.GetClusterConfig().GetZkAddrs(), ",")
	conn, signal, err := zk.Connect(addresses, time.Second, option) //*10)
	if err != nil {
		return err
	}

	if app.GetClusterConfig().GetZkAuthentication() != "" {
		err = conn.AddAuth("digest", []byte(app.GetClusterConfig().GetZkAuthentication()))
		if err != nil {
			return err
		}
	}

	if err := zkutils.CreateNodeIfNecessary(path.GetRootPath(), conn); err != nil {
		return err
	}

	_zkConn = conn
	_zkConnSignal = signal
	_zkAddresses = addresses

	return nil
}

func Close() {
	if _zkConn != nil {
		_zkConn.Close()
	}
}

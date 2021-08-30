package datasource

import (
	"fmt"

	"github.com/siddontang/go-mysql/canal"

	"go-mysql-transfer/model/po"
)

func CreateCanal(ds *po.SourceInfo) (*canal.Canal, error) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", ds.GetHost(), ds.GetPort())
	cfg.User = ds.GetUsername()
	cfg.Password = ds.GetPassword()
	cfg.Flavor = ds.GetFlavor()
	if ds.GetCharset() != "" {
		cfg.Charset = ds.GetCharset()
	}
	if ds.GetSlaveID() != 0 {
		cfg.ServerID = ds.GetSlaveID()
	}
	cfg.Dump.DiscardErr = false
	cfg.Dump.ExecutionPath = ""
	//cfg.Dump.SkipMasterData = global.Cfg().SkipMasterData

	canal, err := canal.NewCanal(cfg)
	if err != nil {
		return nil, err
	}

	return canal, nil
}

func CloseCanal(cc *canal.Canal) {
	if cc != nil {
		cc.Close()
	}
}

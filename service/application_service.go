package service

import (
	sidlog "github.com/siddontang/go-log/log"

	"go-mysql-transfer/global"
	"go-mysql-transfer/storage"
	"go-mysql-transfer/util/logutil"
)

var (
	_transferServiceIns *TransferService
	_clusterServiceIns  *ClusterService
)

func InitApplication(cfgPath string) error {
	cfg, err := global.NewConfigWithFile(cfgPath)
	if err != nil {
		return err
	}

	err = logutil.InitGlobalLogger(cfg.LoggerConfig)
	if err != nil {
		return err
	}

	logutil.BothInfof("source  %s(%s)", cfg.Flavor, cfg.Addr)
	logutil.BothInfof("destination %s", cfg.Destination())

	var streamHandler *sidlog.StreamHandler
	streamHandler, err = sidlog.NewStreamHandler(logutil.GlobalLogWriter())
	if err != nil {
		return err
	}
	agent := sidlog.New(streamHandler, sidlog.Ltime|sidlog.Lfile|sidlog.Llevel)
	sidlog.SetDefaultLogger(agent)

	err = storage.InitStorage(cfg)
	if err != nil {
		return err
	}

	transferService := &TransferService{
		config: cfg,
	}
	err = transferService.initialize()
	if err != nil {
		return err
	}
	_transferServiceIns = transferService

	_clusterServiceIns = &ClusterService{}

	return nil
}

func CtxDone() <-chan struct{} {
	return _transferServiceIns.ctx.Done()
}

func CtxErr() error {
	return _transferServiceIns.ctx.Err()
}

func TransferServiceIns() *TransferService {
	return _transferServiceIns
}

func StartApplication() {
	go _transferServiceIns.run()
}

func CloseApplication() {
	_transferServiceIns.close()
	storage.CloseStorage()
}

func BootCluster() error {
	return _clusterServiceIns.boot(global.Cfg())
}

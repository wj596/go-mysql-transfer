/*
 * Copyright 2020-2021 the original author(https://github.com/wj596)
 *
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */
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

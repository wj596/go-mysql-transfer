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
package main

import (
	"fmt"
	"go-mysql-transfer/config"
	"go-mysql-transfer/dao"
	"go-mysql-transfer/service"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/juju/errors"

	"go-mysql-transfer/admin/web"
	log2 "go-mysql-transfer/util/log"
)

var (
	helpFlag     bool
	cfgPath      string
	stockFlag    bool
	positionFlag bool
	statusFlag   bool
)

func main() {
	configFile := "D:\\newtransfers\\application.yml"

	// 初始化Config
	log.Println(fmt.Sprintf("初始化系统配置：%s", configFile))
	if err := config.Initialize(configFile); err != nil {
		println(errors.ErrorStack(err))
		return
	}

	// 初始化Logger
	if err := log2.Initialize(config.GetIns().GetLoggerConfig()); err != nil {
		println(errors.ErrorStack(err))
		return
	}

	// 初始化DAO层
	if err := dao.Initialize(config.GetIns()); err != nil {
		println(errors.ErrorStack(err))
		return
	}

	// 初始化Metrics

	// 启动RPC服务

	// 启动集群服务

	//初始化service
	if err := service.Initialize(); err != nil {
		println(errors.ErrorStack(err))
		return
	}

	// 启动WEB服务
	if err := web.Initialize(); err != nil {
		println(errors.ErrorStack(err))
		return
	}

	s := make(chan os.Signal, 1)
	signal.Notify(s, os.Kill, os.Interrupt, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	signal := <-s
	log.Printf("应用停止，中断信号为: %s \n", signal.String())

	// 关闭WEB服务
	web.Close()
	//service.Close()
	//storage.Close()
}

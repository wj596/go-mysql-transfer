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
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"regexp"
	"syscall"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"

	"go-mysql-transfer/global"
	"go-mysql-transfer/metrics"
	"go-mysql-transfer/service"
	"go-mysql-transfer/storage"
	"go-mysql-transfer/util/stringutil"
	"go-mysql-transfer/web"
)

var (
	helpFlag     bool
	cfgPath      string
	stockFlag    bool
	positionFlag bool
	statusFlag   bool
)

func init() {
	flag.BoolVar(&helpFlag, "help", false, "this help")
	flag.StringVar(&cfgPath, "config", "app.yml", "application config file")
	flag.BoolVar(&stockFlag, "stock", false, "stock data import")
	flag.BoolVar(&positionFlag, "position", false, "set dump position")
	flag.BoolVar(&statusFlag, "status", false, "display application status")
	flag.Usage = usage
}

func main() {

	//stockFlag = true

	//cfgPath = "D:\\transfer\\app.yml"

	//cfgPath = "D:\\transfer\\release_test_luascript.yml"

	//cfgPath = "D:\\transfer\\rabbitmq_release_test_lua.yml"
	//cfgPath = "D:\\transfer\\rabbitmq_release_test_rule.yml"

	// cfgPath = "D:\\transfer\\kafka_release_test_lua.yml"
	// cfgPath = "D:\\transfer\\kafka_release_test_rule.yml"

	//cfgPath = "D:\\transfer\\rocketmq_release_test_lua.yml"
	//cfgPath = "D:\\transfer\\rocketmq_release_test_rule.yml"

	//cfgPath = "D:\\transfer\\es7_release_test_lua.yml"
	//cfgPath = "D:\\transfer\\es7_release_test_rule.yml"

	//cfgPath = "D:\\transfer\\es6_release_test_lua.yml"
	//cfgPath = "D:\\transfer\\es6_release_test_rule.yml"

	//cfgPath = "D:\\transfer\\redis_release_test_lua.yml"
	//cfgPath = "D:\\transfer\\redis_release_test_rule.yml"

	//cfgPath = "D:\\transfer\\mongo_release_test_rule.yml"
	//cfgPath = "D:\\transfer\\mongo_release_test_lua.yml"

	flag.Parse()
	if helpFlag {
		flag.Usage()
		return
	}

	// 初始化global
	err := global.Initialize(cfgPath)
	if err != nil {
		println(errors.ErrorStack(err))
		return
	}

	if stockFlag {
		doStock()
		return
	}

	// 初始化Storage
	err = storage.Initialize()
	if err != nil {
		println(errors.ErrorStack(err))
		return
	}

	if statusFlag {
		doStatus()
		return
	}

	if positionFlag {
		doPosition()
		return
	}

	err = service.Initialize()
	if err != nil {
		println(errors.ErrorStack(err))
		return
	}

	if err := metrics.Initialize(); err != nil {
		println(errors.ErrorStack(err))
		return
	}

	if err := web.Start(); err != nil {
		println(errors.ErrorStack(err))
		return
	}
	service.StartUp() // start application

	s := make(chan os.Signal, 1)
	signal.Notify(s, os.Kill, os.Interrupt, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	sin := <-s
	log.Printf("application stoped，signal: %s \n", sin.String())

	web.Close()
	service.Close()
	storage.Close()
}

func doStock() {
	stock := service.NewStockService()
	if err := stock.Run(); err != nil {
		println(errors.ErrorStack(err))
	}
	stock.Close()
}

func doStatus() {
	ps := storage.NewPositionStorage()
	pos, _ := ps.Get()
	fmt.Printf("The current dump position is : %s %d \n", pos.Name, pos.Pos)
}

func doPosition() {
	others := flag.Args()
	if len(others) != 2 {
		println("error: please input the binlog's File and Position")
		return
	}
	f := others[0]
	p := others[1]

	matched, _ := regexp.MatchString(".+\\.\\d+$", f)
	if !matched {
		println("error: The parameter File must be like: mysql-bin.000001")
		return
	}

	pp, err := stringutil.ToUint32(p)
	if nil != err {
		println("error: The parameter Position must be number")
		return
	}
	ps := storage.NewPositionStorage()
	pos := mysql.Position{
		Name: f,
		Pos:  pp,
	}
	ps.Save(pos)
	fmt.Printf("The current dump position is : %s %d \n", f, pp)
}

func usage() {
	fmt.Fprintf(os.Stderr, `version: 1.0.0
Usage: transfer [-c filename] [-s stock]

Options:
`)
	flag.PrintDefaults()
}

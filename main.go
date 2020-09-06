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
	"runtime"
	"strings"
	"syscall"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"

	"go-mysql-transfer/global"
	"go-mysql-transfer/service"
	"go-mysql-transfer/storage"
	"go-mysql-transfer/util/logutil"
	"go-mysql-transfer/util/stringutil"
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
	flag.Parse()
	if helpFlag {
		flag.Usage()
		return
	}

	n := runtime.GOMAXPROCS(runtime.NumCPU())
	log.Println(fmt.Sprintf("GOMAXPROCS :%d", n))

	///cfgPath = "D:\\transfer\\app_mongo_lua.yml"
	///stockFlag = true

	err := service.InitApplication(cfgPath)
	if err != nil {
		println(errors.ErrorStack(err))
		return
	}

	logutil.Infof("GOMAXPROCS :%d ", n)

	if statusFlag {
		ps := storage.NewPositionStorage(global.Cfg())
		pos, _ := ps.Get()
		fmt.Printf("The current dump position is : %s %d \n", pos.Name, pos.Pos)
		return
	}

	if positionFlag {
		others := flag.Args()
		if len(others) != 2 {
			println("error: please input the binlog's File and Position")
			return
		}
		f := others[0]
		p := others[1]
		if !strings.HasPrefix(f, "mysql-bin.") {
			println("error: The parameter File must be like: mysql-bin.000001")
			return
		}
		pp, err := stringutil.ToUint32(p)
		if nil != err {
			println("error: The parameter Position must be number")
			return
		}
		ps := storage.NewPositionStorage(global.Cfg())
		pos := mysql.Position{
			Name: f,
			Pos:  pp,
		}
		ps.Save(pos)
		fmt.Printf("The current dump position is : %s %d \n", f, pp)
		return
	}

	if stockFlag {
		transfer := service.TransferServiceIns()
		stock := service.NewStockService(transfer)
		err = stock.Run()
		if err != nil {
			println(errors.ErrorStack(err))
		}
		stock.Close()
		return
	}

	global.StartMonitor()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan,
		os.Kill,
		os.Interrupt,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	if global.Cfg().NotCluster() {
		service.StartApplication()
	} else {
		service.BootCluster()
	}

	select {
	case sig := <-signalChan:
		log.Printf("Application Stop，Signal: %s \n", sig.String())
	case <-service.CtxDone():
		log.Printf("context is done with %v, closing", service.CtxErr())
	}

	service.CloseApplication()
}

func usage() {
	fmt.Fprintf(os.Stderr, `version: 1.0.0
Usage: transfer [-c filename] [-s stock]

Options:
`)
	flag.PrintDefaults()
}

package main

import (
	"flag"
	"fmt"
	"github.com/juju/errors"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"go-mysql-transfer/global"
	"go-mysql-transfer/service"
	"go-mysql-transfer/util/logutil"
)

var (
	helpFlag  bool
	cfgPath   string
	stockFlag bool
)

func init() {
	flag.BoolVar(&helpFlag, "h", false, "this help")
	flag.StringVar(&cfgPath, "config", "app.yml", "application config file")
	flag.BoolVar(&stockFlag, "stock", false, "stock data import")
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

	stockFlag = true
	cfgPath = "D:\\stock\\app.yml"

	err := service.InitApplication(cfgPath)
	if err != nil {
		println(errors.ErrorStack(err))
		return
	}

	logutil.Infof("GOMAXPROCS :%d ", n)

	if stockFlag {
		log.Println("Start stock ... ")
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
	global.SetApplicationState(global.MetricsStateOK)

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

	sig := <-signalChan
	log.Printf("Application Stop，Signal: %s \n", sig.String())

	service.CloseApplication()
	global.SetApplicationState(global.MetricsStateNO)
}

func usage() {
	fmt.Fprintf(os.Stderr, `version: 1.0.0
Usage: transfer [-c filename] [-s stock]

Options:
`)
	flag.PrintDefaults()
}

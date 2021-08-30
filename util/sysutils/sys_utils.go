package sysutils

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/asaskevich/govalidator"
)

type Closer interface {
	Close()
}

// CurrentDirectory 获取程序运行路径
func CurrentDirectory() string {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		fmt.Errorf(err.Error())
	}
	return strings.Replace(dir, "\\", "/", -1)
}

func WaitCloseSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, os.Kill, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	<-signals
}

func WaitCloseSignalsAndRelease(closer Closer) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, os.Kill, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	<-signals
	closer.Close()
}

func IsAddresses(addresses string) bool {
	arrays := strings.Split(addresses, ",")
	for _, address := range arrays {
		lastIndex := strings.LastIndex(address, ":")
		if lastIndex == -1 {
			return false
		}
		ip := address[0:lastIndex]
		port := address[lastIndex+1:]
		if !govalidator.IsIP(ip) {
			return false
		}
		if !govalidator.IsNumeric(port) {
			return false
		}
	}
	return true
}

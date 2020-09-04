package netutil

import (
	"fmt"
	"testing"
)

func TestGetFreePort(t *testing.T) {
	fmt.Println(GetFreePort("127.0.0.1"))
}

func TestGetIp(t *testing.T) {
	fmt.Println(GetIpList())
}

func TestCheckIp(t *testing.T) {
	fmt.Println(CheckIp("192.168.56.1"))
	fmt.Println(CheckIp("192.168.56"))
	fmt.Println(CheckIp("192.168.56.300"))
}

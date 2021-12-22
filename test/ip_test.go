package test

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
)

func inet_aton(ipnr net.IP) int64 {
	bits := strings.Split(ipnr.String(), ".")
	b0, _ := strconv.Atoi(bits[0])
	b1, _ := strconv.Atoi(bits[1])
	b2, _ := strconv.Atoi(bits[2])
	b3, _ := strconv.Atoi(bits[3])
	var sum int64
	sum += int64(b0) << 24
	sum += int64(b1) << 16
	sum += int64(b2) << 8
	sum += int64(b3)
	return sum
}

func TestIp(t *testing.T) {
	fmt.Println(inet_aton(net.ParseIP("10.1.42.132")))
	fmt.Println(inet_aton(net.ParseIP("127.0.0.1")))
}

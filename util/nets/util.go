package nets

import (
	"net"
	"regexp"
	"strconv"
	"strings"
)

// 检测是否为 IP 格式
func CheckIp(addr string) bool {
	if "" == addr {
		return false
	}

	a := net.ParseIP(addr)
	if a == nil {
		return false
	}

	return true
}

// 检测 地址是否为 IP:端口 格式
func CheckHostAddr(addr string) bool {

	if "" == addr {
		return false
	}

	items := strings.Split(addr, ":")
	if items == nil || len(items) != 2 {
		return false
	}

	a := net.ParseIP(items[0])
	if a == nil {
		return false
	}

	match, err := regexp.MatchString("^[0-9]*$", items[1])
	if err != nil {
		return false
	}

	i, err := strconv.Atoi(items[1])
	if err != nil {
		return false
	}
	if i < 0 || i > 65535 {
		return false
	}

	if match == false {
		return false
	}
	return true
}

// 获取一个空闲的TCP端口
func GetFreePort(bind string) int {
	ip := ":"
	if "" != bind {
		ip = bind + ":"
	}

	var port int
	for i := 17070; i < 65536; i++ {
		addr, _ := net.ResolveTCPAddr("tcp", ip+strconv.Itoa(i))
		listener, err := net.ListenTCP("tcp", addr)
		if err == nil {
			listener.Close()
			port = i
			break
		}
	}
	return port
}

func IsUsableTcpAddr(addr string) (bool, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return false, err
	}
	listener.Close()
	return true, nil
}

func IsActiveTCPAddr(addr string) (bool, error) {
	tcpAddr, _ := net.ResolveTCPAddr("tcp", addr)
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return false, err
	}
	defer conn.Close()

	return true, nil
}

func GetIpList() ([]string, error) {
	var ips []string
	netInterfaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(netInterfaces); i++ {
		if (netInterfaces[i].Flags & net.FlagUp) != 0 {
			ls, _ := netInterfaces[i].Addrs()
			for _, address := range ls {
				if ipNet, ok := address.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
					if ipNet.IP.To4() != nil {
						ips = append(ips, ipNet.IP.String())
					}
				}
			}
		}
	}
	return ips, nil
}

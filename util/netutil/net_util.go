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
package netutil

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

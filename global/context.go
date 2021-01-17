package global

import (
	"fmt"
	"log"
	"runtime"
	"strconv"
	"syscall"
	"time"

	sidlog "github.com/siddontang/go-log/log"
	"go-mysql-transfer/util/logs"
)

var (
	_pid         int
	_coordinator int
	_leaderFlag  bool
	_leaderNode  string
	_currentNode string
	_bootTime    time.Time
)

func SetLeaderFlag(flag bool) {
	_leaderFlag = flag
}

func IsLeader() bool {
	return _leaderFlag
}

func SetLeaderNode(leader string) {
	_leaderNode = leader
}

func LeaderNode() string {
	return _leaderNode
}

func CurrentNode() string {
	return _currentNode
}

func IsFollower() bool {
	return !_leaderFlag
}

func BootTime() time.Time {
	return _bootTime
}

func Initialize(configPath string) error {
	if err := initConfig(configPath); err != nil {
		return err
	}
	runtime.GOMAXPROCS(_config.Maxprocs)

	// 初始化global logger
	if err := logs.Initialize(_config.LoggerConfig); err != nil {
		return err
	}

	streamHandler, err := sidlog.NewStreamHandler(logs.Writer())
	if err != nil {
		return err
	}
	agent := sidlog.New(streamHandler, sidlog.Ltime|sidlog.Lfile|sidlog.Llevel)
	sidlog.SetDefaultLogger(agent)

	_bootTime = time.Now()
	_pid = syscall.Getpid()

	if _config.IsCluster(){
		if _config.EnableWebAdmin {
			_currentNode = _config.Cluster.BindIp + ":" + strconv.Itoa(_config.WebAdminPort)
		} else {
			_currentNode = _config.Cluster.BindIp + ":" + strconv.Itoa(_pid)
		}
	}

	log.Println(fmt.Sprintf("process id: %d", _pid))
	log.Println(fmt.Sprintf("GOMAXPROCS :%d", _config.Maxprocs))
	log.Println(fmt.Sprintf("source  %s(%s)", _config.Flavor, _config.Addr))
	log.Println(fmt.Sprintf("destination %s", _config.Destination()))

	return nil
}

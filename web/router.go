package web

import (
	"fmt"
	"go-mysql-transfer/service"
	"go-mysql-transfer/util/dates"
	"go-mysql-transfer/util/nets"
	"log"
	"net/http"
	"path"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"go-mysql-transfer/global"
	"go-mysql-transfer/metrics"
	"go-mysql-transfer/util/logs"
)

var _server *http.Server

func Start() error {
	if !global.Cfg().EnableWebAdmin { //哨兵
		return nil
	}

	gin.SetMode(gin.ReleaseMode)
	g := gin.New()
	//statics := "D:\\statics"
	//index := "D:\\statics\\index.html"

	statics := "statics"
	index := path.Join(statics, "index.html")
	g.Static("/statics", statics)
	g.LoadHTMLFiles(index)
	g.GET("/", webAdminFunc)

	port := global.Cfg().WebAdminPort
	listen := fmt.Sprintf(":%s", strconv.Itoa(port))
	_server = &http.Server{
		Addr:           listen,
		Handler:        g,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	ok, err := nets.IsUsableTcpAddr(listen)
	if !ok {
		return err
	}

	log.Println(fmt.Sprintf("Web Admin Listen At %s", listen))
	go func() {
		if err := _server.ListenAndServe(); err != nil {
			logs.Error(err.Error())
		}
	}()

	return nil
}

func webAdminFunc(c *gin.Context) {
	pos, _ := service.TransferServiceIns().Position()

	var tables []string
	for _, v := range global.RuleKeyList() {
		tables = append(tables, v)
	}

	var insertAmounts []uint64
	for _, v := range tables {
		insertAmounts = append(insertAmounts, metrics.LabInsertAmount(v))
	}

	var updateAmounts []uint64
	for _, v := range tables {
		updateAmounts = append(updateAmounts, metrics.LabUpdateRecord(v))
	}

	var deleteAmounts []uint64
	for _, v := range tables {
		deleteAmounts = append(deleteAmounts, metrics.LabDeleteRecord(v))
	}

	h := gin.H{
		"mysql":         global.Cfg().Addr,
		"binName":       pos.Name,
		"binPos":        pos.Pos,
		"destName":      global.Cfg().DestStdName(),
		"destAddr":      global.Cfg().DestAddr(),
		"destState":     metrics.DestState(),
		"bootTime":      dates.Layout(global.BootTime(), dates.DayTimeMinuteFormatter),
		"insertAmount":  metrics.InsertAmount(),
		"updateAmount":  metrics.UpdateAmount(),
		"deleteAmount":  metrics.DeleteAmount(),
		"tables":        tables,
		"insertAmounts": insertAmounts,
		"updateAmounts": updateAmounts,
		"deleteAmounts": deleteAmounts,
		"isCluster":     global.Cfg().IsCluster(),
		"isRedirect":    false,
	}

	if global.Cfg().IsCluster() {
		h["isZk"] = global.Cfg().IsZk()
		h["zkAddrs"] = global.Cfg().Cluster.ZkAddrs
		h["etcdAddrs"] = global.Cfg().Cluster.EtcdAddrs
		h["isLeader"] = global.IsLeader()
		h["leader"] = global.LeaderNode()
		if !global.IsLeader() {
			h["isRedirect"] = true
		}

		var followers []string
		for _, v := range service.ClusterServiceIns().Nodes() {
			if v != global.LeaderNode() {
				followers = append(followers, v)
			}
		}
		h["followers"] = followers
	}

	c.HTML(200, "index.html", h)
}

func Close() {
	if _server == nil {
		return
	}

	err := _server.Shutdown(nil)
	if err != nil {
		log.Println(err.Error())
	}
}

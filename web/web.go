/*
 * Copyright 2021-2022 the original author(https://github.com/wj596)
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

package web

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"go-mysql-transfer/config"
	"go-mysql-transfer/util/netutils"
	"go-mysql-transfer/web/filter"
)

var _server *http.Server

func Initialize() error {
	gin.SetMode(gin.ReleaseMode)
	handler := gin.New()
	handler.Use(filter.CorsFilter())
	setStatics(handler)

	// admin 模块
	{
		admin := handler.Group("/console")
		admin.Use(filter.AuthFilter())
		initAuthAction(admin)
		initSourceInfoAction(admin)
		initEndpointInfoAction(admin)
		initPipelineInfoAction(admin)
		initRunningAction(admin)
		initClusterAction(admin)
		initDashboardAction(admin)
	}

	// 集群模块
	{
		cluster := handler.Group("/cluster")
		cluster.Use(filter.SignFilter())
		initLeaderAction(cluster)
		initFollowerAction(cluster)
	}

	port := config.GetIns().GetWebPort()
	listen := fmt.Sprintf(":%s", strconv.Itoa(port))
	_server = &http.Server{
		Addr:           listen,
		Handler:        handler,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	if ok, err := netutils.IsUsableTcpAddr(listen); !ok {
		return err
	}

	go func() {
		log.Println(fmt.Sprintf("Web Admin Listen At %s", listen))
		if err := _server.ListenAndServe(); err != nil {
			log.Println(err.Error())
		}
	}()

	return nil
}

func setStatics(handler *gin.Engine) {
	//current := fileutils.GetCurrentDirectory()
	//handler.LoadHTMLFiles(current + "/console/index.html")
	//handler.StaticFile("/favicon.ico", current+"/console/favicon.ico")
	//handler.StaticFile("/_app.config.js", current+"/console/_app.config.js")
	//handler.StaticFS("/assets", http.Dir(current+"/console/assets"))
	//handler.StaticFS("/resource", http.Dir(current+"/console/resource"))
	//handler.GET("/", func(c *gin.Context) {
	//	c.HTML(http.StatusOK, "index.html", gin.H{})
	//})

	handler.LoadHTMLFiles("D:\\dev\\nodejs\\workspace\\go-mysql-transfer-ui\\dist\\index.html")
	handler.StaticFile("/favicon.ico", "D:\\dev\\nodejs\\workspace\\go-mysql-transfer-ui\\dist\\favicon.ico")
	handler.StaticFile("/_app.config.js", "D:\\dev\\nodejs\\workspace\\go-mysql-transfer-ui\\dist\\_app.config.js")
	handler.StaticFS("/assets", http.Dir("D:\\dev\\nodejs\\workspace\\go-mysql-transfer-ui\\dist\\assets"))
	handler.StaticFS("/resource", http.Dir("D:\\dev\\nodejs\\workspace\\go-mysql-transfer-ui\\dist\\resource"))
	handler.GET("/", func(c *gin.Context) {
		c.HTML(http.StatusOK, "index.html", gin.H{})
	})
}

func Err400(c *gin.Context, message string) {
	respErr(c, http.StatusBadRequest, message)
}

func Err401(c *gin.Context, message string) {
	respErr(c, http.StatusUnauthorized, message)
}

func Err403(c *gin.Context, message string) {
	respErr(c, http.StatusForbidden, message)
}

func Err500(c *gin.Context, message string) {
	respErr(c, http.StatusInternalServerError, message)
}

func respErr(c *gin.Context, status int, message string) {
	c.JSON(status, NewErrorResp().SetMessage(message))
}

func RespOK(c *gin.Context) {
	c.JSON(http.StatusOK, NewSuccessResp().SetMessage("操作成功"))
}

func RespMsg(c *gin.Context, message string) {
	c.JSON(http.StatusOK, NewSuccessResp().SetMessage(message))
}

func RespData(c *gin.Context, data interface{}) {
	c.JSON(http.StatusOK, NewSuccessResp().SetResult(data))
}

func GetToken(c *gin.Context) string {
	return c.Request.Header.Get("Authorization")
}

func Close() {
	if _server == nil {
		return
	}

	ctx := context.Background()
	err := _server.Shutdown(ctx)
	if err != nil {
		log.Println(err.Error())
	}
}

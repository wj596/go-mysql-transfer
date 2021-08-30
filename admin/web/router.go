package web

import (
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"go-mysql-transfer/config"
	"go-mysql-transfer/util/netutils"
)

var _server *http.Server

func Initialize() error {
	gin.SetMode(gin.ReleaseMode)
	handler := gin.New()
	handler.Use(CorsFilter())
	//handler.Use(middleware.Auth())
	initAuthAction(handler)
	initSourceInfoAction(handler)
	initEndpointInfoAction(handler)
	initPipelineInfoAction(handler)
	initTransformRuleAction(handler)

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

	err := _server.Shutdown(nil)
	if err != nil {
		log.Println(err.Error())
	}
}

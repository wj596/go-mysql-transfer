package filter

import (
	"github.com/gin-gonic/gin"
	"go-mysql-transfer/service"
)

// AuthFilter 鉴权中间件
func AuthFilter() gin.HandlerFunc {
	return func(c *gin.Context) {
		path := c.Request.URL.Path
		token := c.Request.Header.Get("Authorization")
		if "/console/auths/login" == path {
			c.Next()
			return
		}

		if "" == token {
			c.AbortWithStatus(401)
			return
		}

		err := service.GetAuthService().Validate(token)
		if nil != err {
			c.AbortWithStatus(401)
			return
		}
		c.Next()
	}
}

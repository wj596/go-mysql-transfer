package filter

import (
	"github.com/gin-gonic/gin"
)

// CorsFilter 跨域中间件
func CorsFilter() gin.HandlerFunc {
	return func(c *gin.Context) {
		origin := c.Request.Header.Get("origin")
		if "" == origin {
			origin = c.Request.Header.Get("Referer")
		}

		c.Writer.Header().Set("Access-Control-Allow-Origin", origin)
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Max-Age", "18000")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT,DELETE,OPTIONS,PATCH")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, XMLHttpRequest, Accept-Encoding, X-CSRF-Token, Authorization")

		if c.Request.Method == "OPTIONS" { // 放行所有OPTIONS方法
			c.AbortWithStatus(200)
			return
		}

		c.Next()
	}
}

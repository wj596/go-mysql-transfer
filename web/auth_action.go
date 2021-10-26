package web

import (
	"github.com/gin-gonic/gin"

	"go-mysql-transfer/domain/vo"
	"go-mysql-transfer/service"
)

type AuthAction struct {
	authService *service.AuthService
}

func initAuthAction(r *gin.Engine) {
	s := &AuthAction{
		authService: service.GetAuthService(),
	}
	r.GET("auths/ping", s.Ping)
	r.POST("auths/login", s.Login)
	r.GET("auths/authenticated", s.Authenticated)
	r.GET("auths/logout", s.Logout)
	r.GET("auths/validate", s.Validate)
}

func (s *AuthAction) Ping(c *gin.Context) {
	RespMsg(c, "Pong")
}

func (s *AuthAction) Login(c *gin.Context) {
	var params vo.LoginVO
	c.BindJSON(&params)

	if params.Username == "" || params.Password == "" {
		Err400(c, "用户名和密码均不能为空")
		return
	}

	token, err := s.authService.Login(params.Username, params.Password)
	if err != nil {
		Err401(c, err.Error())
		return
	}

	RespData(c, gin.H{
		"token": token,
	})
}

func (s *AuthAction) Authenticated(c *gin.Context) {
	token := GetToken(c)
	if "" == token {
		c.AbortWithStatus(401)
		return
	}

	session, err := s.authService.GetSession(token)
	if err != nil {
		Err401(c, err.Error())
		return
	}

	RespData(c, gin.H{
		"username": session.Name,
		"realName": session.Name,
		"roles": []*vo.RoleVO{&vo.RoleVO{
			RoleName: session.Role,
			Value:    session.Role,
		}},
	})
}

func (s *AuthAction) Logout(c *gin.Context) {
	token := GetToken(c)
	if "" == token {
		c.AbortWithStatus(401)
		return
	}

	RespOK(c)
}

func (s *AuthAction) Validate(c *gin.Context) {
	token := GetToken(c)
	if "" == token {
		c.AbortWithStatus(401)
		return
	}

	RespData(c, gin.H{})
}

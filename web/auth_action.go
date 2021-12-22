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
	"github.com/gin-gonic/gin"

	"go-mysql-transfer/domain/vo"
	"go-mysql-transfer/service"
)

type AuthAction struct {
	authService *service.AuthService
}

func initAuthAction(r *gin.RouterGroup) {
	s := &AuthAction{
		authService: service.GetAuthService(),
	}
	r.POST("auths/login", s.Login)
	r.GET("auths/authenticated", s.Authenticated)
	r.GET("auths/logout", s.Logout)
	r.GET("auths/validate", s.Validate)
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

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

package service

import (
	"errors"
	"sync"
	"time"

	"go-mysql-transfer/config"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/stringutils"
)

const (
	_sessionTimeout = 1800 //默认session超时时间1800秒
)

type Session struct {
	Name       string //用户名
	Role       string //角色 admin|viewer
	ActiveTime int64  //活跃时间
}

type AuthService struct {
	sessionMap       map[string]*Session
	lockOfSessionMap sync.RWMutex
}

func (s *AuthService) Login(name string, password string) (string, error) {
	var user *config.UserConfig
	for _, consumer := range config.GetIns().GetUserConfigs() {
		if consumer.GetName() == name && consumer.GetPassword() == password {
			user = consumer
			break
		}
	}
	if user == nil {
		return "", errors.New("用户名或密码不正确")
	}

	token := stringutils.UUID()
	session := &Session{
		Name:       user.GetName(),
		Role:       user.GetRole(),
		ActiveTime: time.Now().Unix(),
	}

	s.lockOfSessionMap.RLock()
	s.sessionMap[token] = session
	s.lockOfSessionMap.RUnlock()

	log.Infof("user:%s logined", session.Name)

	s.cleanupInvalidSession()

	return token, nil
}

func (s *AuthService) Logout(token string) {
	session, exist := s.sessionMap[token]
	if exist {
		log.Infof("user:%s logout", session.Name)
		s.lockOfSessionMap.Lock()
		delete(s.sessionMap, token)
		s.lockOfSessionMap.Unlock()
	}
}

func (s *AuthService) Validate(token string) error {
	s.lockOfSessionMap.RLock()
	defer s.lockOfSessionMap.RUnlock()

	session, exist := s.sessionMap[token]
	if !exist {
		return errors.New("无效的token")
	}
	session.ActiveTime = time.Now().Unix()

	return nil
}

// GetSession 获取会话
func (s *AuthService) GetSession(token string) (*Session, error) {
	s.lockOfSessionMap.RLock()
	defer s.lockOfSessionMap.RUnlock()

	session, exist := s.sessionMap[token]
	if !exist {
		return nil, errors.New("无效的token")
	}
	session.ActiveTime = time.Now().Unix()

	return session, nil
}

// cleanupInvalidSession 清理过期Token
func (s *AuthService) cleanupInvalidSession() {
	overdueList := make([]string, 0)
	for k, v := range s.sessionMap {
		if time.Now().Unix()-v.ActiveTime >= _sessionTimeout {
			overdueList = append(overdueList, k)
		}
	}

	for _, v := range overdueList {
		s.lockOfSessionMap.Lock()
		delete(s.sessionMap, v)
		s.lockOfSessionMap.Unlock()
	}
}

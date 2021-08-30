package service

import (
	"errors"
	"fmt"
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
	var logined *config.ConsumerConfig
	for _, consumer := range config.GetIns().GetConsumerConfigs() {
		if consumer.GetName() == name && consumer.GetPassword() == password {
			logined = consumer
			break
		}
	}
	if logined == nil {
		return "", errors.New("用户名或密码不正确")
	}

	token := stringutils.UUID()
	session := &Session{
		Name:       logined.GetName(),
		Role:       logined.GetRole(),
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
		fmt.Println(fmt.Sprintf("清理过期Session: %s", v))
	}
}

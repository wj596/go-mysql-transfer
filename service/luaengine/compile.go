package luaengine

import (
	"errors"
	"io/ioutil"
	"path/filepath"
	"strings"
	"sync"

	lua "github.com/yuin/gopher-lua"
	"github.com/yuin/gopher-lua/parse"

	"go-mysql-transfer/global"
	"go-mysql-transfer/util/fileutil"
)

var (
	protoMap       = make(map[string]*lua.FunctionProto)
	lockOfProtoMap sync.Mutex
)

func PreCompile(rule *global.Rule, conf *global.Config) error {
	lockOfProtoMap.Lock()
	defer lockOfProtoMap.Unlock()

	script := rule.LuaScript
	if rule.LuaFilePath != "" {
		var filePath string
		if fileutil.IsExist(rule.LuaFilePath) {
			filePath = rule.LuaFilePath
		} else {
			filePath = filepath.Join(conf.DataDir, rule.LuaFilePath)
		}
		data, err := ioutil.ReadFile(filePath)
		if err != nil {
			return err
		}
		script = string(data)
	}

	if script == "" {
		return errors.New("empty lua script not allowed")
	}

	if !strings.Contains(script, "function") {
		return errors.New("lua script incorrect format")
	}

	if !strings.Contains(script, "transfer(") {
		return errors.New("lua script incorrect format")
	}

	if !strings.Contains(script, "transfer(") {
		return errors.New("lua script incorrect format")
	}

	if conf.IsRedis() {
		if !strings.Contains(script, `require("redisOps")`) {
			return errors.New("lua script incorrect format")
		}
		switch rule.RedisStructure {
		case global.RedisStructureString:
			if !strings.Contains(script, "SET(") {
				return errors.New("lua script incorrect format")
			}
		case global.RedisStructureHash:
			if !strings.Contains(script, "HSET(") {
				return errors.New("lua script incorrect format")
			}
		case global.RedisStructureList:
			if !strings.Contains(script, "RPUSH(") {
				return errors.New("lua script incorrect format")
			}
		case global.RedisStructureSet:
			if !strings.Contains(script, "SADD(") {
				return errors.New("lua script incorrect format")
			}
		}
	}

	reader := strings.NewReader(script)
	chunk, err := parse.Parse(reader, script)
	if err != nil {
		return err
	}

	var proto *lua.FunctionProto
	proto, err = lua.Compile(chunk, script)
	if err != nil {
		return err
	}

	ruleKey := global.RuleKey(rule.Schema, rule.Table)
	protoMap[ruleKey] = proto

	return nil
}

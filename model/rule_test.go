package model

import (
	"encoding/json"
	"fmt"
	"go-mysql-transfer/model/po"
	"google.golang.org/protobuf/proto"
	"io/ioutil"
	"testing"
)

func TestRuleSerialize(t *testing.T) {
	bo := &po.TransformRule{
		Id: 1,
		Schema:"seap",
		Table: "t_user",

		RedisStructure: 1,
		RedisKeyColumn: "id",
	}

	data, _ := proto.Marshal(bo)
	fmt.Println(len(data))
	data2, _ := json.Marshal(bo)
	fmt.Println(len(data2))
}

func TestRuleLuaScript(t *testing.T) {
	bo := &po.TransformRule{
		Id: 1,
		Schema:"seap",
		Table: "t_user",
	}

	fileData,_ := ioutil.ReadFile("D:\\transfers\\release\\liunx\\transfer\\examples\\lua\\t_user_kafka.lua");

	// fmt.Println(string(fileData))

	bo.LuaScript = string(fileData)

	data, _ := proto.Marshal(bo)
	fmt.Println(len(data))
	data2, _ := json.Marshal(bo)
	fmt.Println(len(data2))

	entity := po.TransformRule{}
	proto.Unmarshal(data, &entity)
	fmt.Println(entity.LuaScript)
}
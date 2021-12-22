package lua

import (
	"fmt"
	canallog "github.com/siddontang/go-log/log"
	lua "github.com/yuin/gopher-lua"
	"go-mysql-transfer/config"
	"go-mysql-transfer/dao"
	"go-mysql-transfer/datasource"
	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/endpoint"
	"go-mysql-transfer/endpoint/luaengine"
	"go-mysql-transfer/service"
	"go-mysql-transfer/util/fileutils"
	zap "go-mysql-transfer/util/log"
	"log"
	"testing"
)

func TestIndex(t *testing.T) {

	configFile := "D:\\newtransfers\\application.yml"
	// 初始化Config
	log.Println(fmt.Sprintf("初始化系统配置：%s", configFile))
	if err := config.Initialize(configFile); err != nil {
		t.Fatal(err)
	}
	// 初始化Logger
	if err := zap.Initialize(config.GetIns().GetLoggerConfig()); err != nil {
		t.Fatal(err)
	}
	// 初始化DAO层
	if err := dao.Initialize(config.GetIns()); err != nil {
		t.Fatal(err)
	}
	//初始化service
	if err := service.Initialize(); err != nil {
		t.Fatal(err)
	}

	canallog.SetLevel(canallog.LevelError)

	endpointService := service.GetEndpointInfoService()
	endpointInfo, _ := endpointService.Get(384899451350679553)
	endpoint := endpoint.NewStreamEndpoint(endpointInfo)
	if err := endpoint.Connect(); err != nil {
		t.Fatal(err)
	}

	sourceService := service.GetSourceInfoService()
	sourceInfo, _ := sourceService.Get(384898879985811457)
	conn, err := datasource.CreateConnection(sourceInfo)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	tableInfo, err := conn.GetTable("eseap", "t_user")
	if err != nil {
		t.Fatal(err)
	}

	luaScript, err := fileutils.ReadAsString("D://test.lua")
	rule := &po.Rule{
		Type: 1,
		LuaScript: luaScript,
	}

	rctx, err := bo.CreateRuleContext(&po.PipelineInfo{
		Id:   11,
		Name: "test",
		EndpointType: 1,
	}, rule, tableInfo, true)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("LuaVM:::",rctx.GetLuaVM())

	L := rctx.GetLuaVM()
	fn := L.GetGlobal(luaengine.HandleFunctionName)
	fmt.Println("fn:::", fn)
	event := L.NewTable()
	err = L.CallByParam(lua.P{
		Fn:     fn,
		NRet:    0,
		Protect: true,
	}, event)
	if err != nil {
		t.Fatal(err)
	}


	//requests := make([]*bo.RowEventRequest, 0)
	//
	//
	//

	//
	//
	//
	//err = endpoint.Stream(requests)
	//if err != nil {
	//	t.Fatal(err)
	//}
}

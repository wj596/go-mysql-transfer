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
	"fmt"
	"go-mysql-transfer/util/jsonutils"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/schema"
	"github.com/yuin/gopher-lua"
	"go.uber.org/atomic"

	"go-mysql-transfer/datasource"
	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/endpoint"
	"go-mysql-transfer/endpoint/lua/luaengine"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/sqlutils"
)

const (
	_streamMonitorInterval = 1 //秒
	_stateFlushInterval    = 3 //秒
)

var (
	_streams       = make(map[uint64]*StreamService)
	_lockOfStreams sync.Mutex
)

type StreamService struct {
	wg                        sync.WaitGroup
	lock                      sync.Mutex
	endpointMonitorStopSignal chan struct{}
	runtime                   *bo.PipelineRuntime

	pipeline     *po.PipelineInfo
	ruleContexts map[string]*bo.RuleContext
	handler      *StreamEventHandler

	endpoint       endpoint.IStreamEndpoint
	endpointType   uint32
	endpointEnable *atomic.Bool

	dumpDatabases []string
	dumpTables    []string
	dumper        *canal.Canal
	dumperConfig  *canal.Config
	dumperEnable  *atomic.Bool
}

func createStreamService(sourceInfo *po.SourceInfo, endpointInfo *po.EndpointInfo, pipeline *po.PipelineInfo, runtime *bo.PipelineRuntime) (*StreamService, error) {
	_lockOfStreams.Lock()
	defer _lockOfStreams.Unlock()

	canalConfig := canal.NewDefaultConfig()
	canalConfig.Addr = fmt.Sprintf("%s:%d", sourceInfo.Host, sourceInfo.Port)
	canalConfig.User = sourceInfo.Username
	canalConfig.Password = sourceInfo.Password
	canalConfig.Charset = sourceInfo.Charset
	canalConfig.Flavor = sourceInfo.Flavor
	canalConfig.ServerID = sourceInfo.SlaveID
	canalConfig.Dump.DiscardErr = false
	canalConfig.Dump.ExecutionPath = ""
	canalConfig.Dump.SkipMasterData = false

	conn, err := datasource.CreateConnection(sourceInfo)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	rules := make([]*po.Rule, 0)
	for _, rule := range pipeline.Rules {
		if !rule.Enable {
			msg, _ := jsonutils.ToJsonIndent(rule)
			log.Infof("管道[%s]忽略同步规则[%s]，已停用", pipeline.Name, msg)
			continue
		}

		if constants.TableTypeSingle == rule.TableType {
			rules = append(rules, rule)
		}
		if constants.TableTypeList == rule.TableType {
			for _, table := range rule.TableList {
				newRule := po.DeepCopyRule(rule)
				newRule.Table = table
				rules = append(rules, newRule)
			}
		}
		if constants.TableTypePattern == rule.TableType {
			list, err := datasource.FilterTableNameList(sourceInfo, rule.Schema, rule.TablePattern)
			if err != nil {
				return nil, err
			}
			for _, table := range list {
				newRule := po.DeepCopyRule(rule)
				newRule.Table = table
				rules = append(rules, newRule)
			}
		}
	}

	schemas := make(map[string]bool)
	tables := make([]string, 0, len(rules))
	ruleContexts := make(map[string]*bo.RuleContext, len(rules))

	for _, rule := range rules {
		var tableInfo *schema.Table
		tableInfo, err = conn.GetTable(rule.Schema, rule.Table)
		if err != nil {
			break
		}
		schemas[rule.Schema] = true
		tables = append(tables, rule.Table)
		canalConfig.IncludeTableRegex = append(canalConfig.IncludeTableRegex, rule.Schema+"\\."+rule.Table)

		var rc *bo.RuleContext
		rc, err = bo.CreateRuleContext(pipeline, rule, tableInfo)
		if err != nil {
			break
		}
		if rc.IsLuaEnable() {
			L := luaengine.New(endpointInfo.GetType())
			err = rc.PreloadLuaVM(L) //预加载Lua虚拟机
			if err != nil {
				break
			}
			dataSourceName := sqlutils.GetDataSourceName(sourceInfo.GetUsername(), sourceInfo.GetPassword(), sourceInfo.GetHost(), "%s", sourceInfo.GetPort(), sourceInfo.GetCharset())
			rc.GetLuaVM().SetGlobal(constants.GlobalDataSourceName, lua.LString(dataSourceName))
		}
		ruleContexts[strings.ToLower(rule.Schema+"."+rule.Table)] = rc
		log.Infof("管道[%s]添加表[%s]同步规则", pipeline.Name, strings.ToLower(rule.Schema+"."+rule.Table))
	}
	if err != nil {
		for _, rc := range ruleContexts {
			rc.CloseLuaVM()
		}
		return nil, err
	}

	dumpDatabases := make([]string, 0, len(schemas))
	for k, _ := range schemas {
		dumpDatabases = append(dumpDatabases, k)
	}

	service := &StreamService{
		endpointMonitorStopSignal: make(chan struct{}, 1),
		runtime:                   runtime,
		pipeline:                  pipeline,
		ruleContexts:              ruleContexts,
		endpoint:                  endpoint.NewStreamEndpoint(endpointInfo),
		endpointType:              endpointInfo.GetType(),
		endpointEnable:            atomic.NewBool(false),
		dumpDatabases:             dumpDatabases,
		dumpTables:                tables,
		dumperConfig:              canalConfig,
		dumperEnable:              atomic.NewBool(false),
	}

	log.Infof("管道[%s]共监听[%d]张表", pipeline.Name, len(ruleContexts))
	_streams[pipeline.Id] = service
	return service, nil
}

func getStreamService(pipelineId uint64) (*StreamService, bool) {
	_lockOfStreams.Lock()
	defer _lockOfStreams.Unlock()

	ser, exist := _streams[pipelineId]
	if !exist {
		return nil, false
	}

	return ser, true
}

func streamServiceClose(serv *StreamService) {
	_lockOfStreams.Lock()
	defer _lockOfStreams.Unlock()

	serv.handler.flushQueue() //刷新缓冲队列
	time.Sleep(1 * time.Second)

	serv.endpointMonitorStopSignal <- struct{}{} //关闭客户端监听
	serv.endpointEnable.Store(false)             //设置客户端不可用状态

	serv.endpoint.Close() //关闭客户端
	serv.endpoint = nil   //help GC

	serv.dumper.Close() //关闭dumper
	serv.wg.Wait()
	serv.dumperEnable.Store(false)
	serv.dumper = nil //help GC

	// 关闭所有Lua VM
	for _, rc := range serv.ruleContexts {
		rc.CloseLuaVM()
	}

	serv.handler.stop() //停止handler
	serv.handler = nil  //help GC

	pipelineId := serv.pipeline.Id
	pipelineName := serv.pipeline.Name
	_stateService.updateStateByClose(pipelineId, serv.runtime)
	delete(_streams, pipelineId)
	serv = nil //help GC
	log.Infof("删除StreamService[%s]", pipelineName)

}

func streamServicePanic(serv *StreamService, cause string) {
	_lockOfStreams.Lock()
	defer _lockOfStreams.Unlock()

	serv.endpointMonitorStopSignal <- struct{}{} //关闭客户端监听
	serv.endpointEnable.Store(false)             //设置客户端不可用状态

	serv.endpoint.Close() //关闭客户端
	serv.endpoint = nil   //help GC

	serv.dumper.Close() //关闭dumper
	serv.wg.Wait()
	serv.dumperEnable.Store(false)
	serv.dumper = nil //help GC

	// 关闭所有Lua VM
	for _, rc := range serv.ruleContexts {
		rc.CloseLuaVM()
	}

	serv.handler.stop() //停止handler
	serv.handler = nil  //help GC

	pipeline := serv.pipeline
	_stateService.updateStateByPanic(pipeline.Id, serv.runtime, cause)
	delete(_streams, pipeline.Id)
	serv = nil //help GC
	log.Infof("删除StreamService[%s]", pipeline.Name)
	_alarmService.panicAlarm(pipeline, cause) //告警
}

func (s *StreamService) startup() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	// 连接接收端
	if err := s.endpoint.Connect(); err != nil {
		return err
	}
	s.endpointEnable.Store(true)

	// 创建Canal
	dumper, err := canal.NewCanal(s.dumperConfig)
	if err != nil {
		return err
	}

	//设置dump的数据库
	if len(s.dumpDatabases) == 1 {
		dumper.AddDumpTables(s.dumpDatabases[0], s.dumpTables...)
	} else {
		dumper.AddDumpDatabases(s.dumpDatabases...)
	}

	handler := newStreamEventHandler(s)
	dumper.SetEventHandler(handler)
	s.handler = handler
	s.dumper = dumper
	position := _positionService.get(s.pipeline.Id)
	err = s.runDumper(position)
	if err != nil {
		return err
	}

	s.startStreamMonitor()
	return nil
}

func (s *StreamService) runDumper(current mysql.Position) error {
	s.handler.start()
	s.dumperEnable.Store(true)
	s.wg.Add(1)
	go func(current mysql.Position) {
		log.Infof("StreamService[%s]从Position[%s %d]启动Dumper", s.pipeline.Name, current.Name, current.Pos)
		if err := s.dumper.RunFrom(current); err != nil {
			s.handler.stop()
		}
		log.Infof("StreamService[%s]销毁Dumper", s.pipeline.Name)
		s.dumperEnable.Store(false)
		s.dumper = nil
		s.wg.Done()
	}(current)

	time.Sleep(1 * time.Second) //canal未提供回调，停留一秒，确保RunFrom启动成功
	if !s.dumperEnable.Load() {
		return errors.Errorf("StreamService[%s]启动Dumper失败", s.pipeline.Name)
	}
	log.Infof("StreamService[%s]启动Dumper成功", s.pipeline.Name)
	_stateService.updateStateByRunning(s.pipeline.Id, s.runtime)
	return nil
}

// 客户端故障
func (s *StreamService) endpointFault(cause string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.dumperEnable.Load() || s.dumper == nil {
		return
	}

	s.endpointEnable.Store(false) //设置客户端不可用状态

	s.dumper.Close() //关闭dumper
	s.wg.Wait()
	s.dumperEnable.Store(false)
	s.dumper = nil //help GC

	s.handler.stop() //停止handler
	s.handler = nil  //help GC

	s.runtime.SetFaultStatus(cause)
	_alarmService.faultAlarm(s.pipeline, cause) //告警
	log.Infof("StreamService[%s]客户端故障[%s]", s.pipeline.Name, cause)
}

// 客户端恢复
func (s *StreamService) endpointResume() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.runtime.IsRunning() || nil != s.dumper {
		return errors.Errorf("StreamService[%s]已启动", s.pipeline.Name)
	}

	dumper, err := canal.NewCanal(s.dumperConfig)
	if err != nil {
		return err
	}

	if len(s.dumpDatabases) == 1 {
		dumper.AddDumpTables(s.dumpDatabases[0], s.dumpTables...)
	} else {
		dumper.AddDumpDatabases(s.dumpDatabases...)
	}
	handler := newStreamEventHandler(s)
	dumper.SetEventHandler(handler)
	s.handler = handler
	s.dumper = dumper
	position := _positionService.get(s.pipeline.Id)
	err = s.runDumper(position)
	log.Infof("StreamService[%s]恢复运行", s.pipeline.Name)
	return err
}

func (s *StreamService) startStreamMonitor() {
	go func() {
		log.Infof("StreamService[%s]启动客户端监控", s.pipeline.Name)
		ticker := time.NewTicker(_streamMonitorInterval * time.Second)
		defer ticker.Stop()
		var err error
		lastStateSaveTime := time.Now()
		for {
			select {
			case <-ticker.C:
				now := time.Now()
				if now.Sub(lastStateSaveTime) > _stateFlushInterval*time.Second {
					lastStateSaveTime = now
					_stateService.updateState(s.pipeline.Id, s.runtime)
				}

				if !s.endpointEnable.Load() { //客户端不可用
					err = s.endpoint.Ping() //ping客户端
					if err == nil {         //ping通
						s.endpointEnable.Store(true) //客户端恢复
						if constants.EndpointTypeRabbitMQ == s.endpointType {
							s.endpoint.Connect()
						}
						err = s.endpointResume() //客户端恢复
						if err != nil {
							log.Errorf("StreamService[%s]恢复运行失败[%s]", s.pipeline.Name, err.Error())
						}
					} else {
						log.Errorf("StreamService[%s]Ping客户端错误[%s]", s.pipeline.Name, err.Error())
					}
				}
			case <-s.endpointMonitorStopSignal:
				log.Infof("StreamService[%s]关闭客户端监控", s.pipeline.Name)
				return
			}
		}
	}()
}

func (s *StreamService) getRuleContext(key string) (*bo.RuleContext, bool) {
	rc, exist := s.ruleContexts[key]
	return rc, exist
}

func (s *StreamService) isEndpointEnable() bool {
	return s.endpointEnable.Load()
}

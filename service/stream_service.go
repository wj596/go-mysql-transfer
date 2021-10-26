package service

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/schema"
	"go.uber.org/atomic"

	"go-mysql-transfer/datasource"
	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/domain/vo"
	"go-mysql-transfer/endpoint"
	"go-mysql-transfer/util/log"
)

const _endpointMonitorInterval = 1

var (
	_streams       = make(map[uint64]*StreamService)
	_lockOfStreams sync.Mutex
)

type StreamService struct {
	wg                        sync.WaitGroup
	lock                      sync.Mutex
	endpointMonitorStopSignal chan struct{}
	runtime                   *bo.PipelineRunState

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

func createStreamService(pipeline *po.PipelineInfo, runtime *bo.PipelineRunState) (*StreamService, error) {
	_lockOfStreams.Lock()
	defer _lockOfStreams.Unlock()

	sourceInfo, err := _sourceInfoService.Get(pipeline.SourceId)
	if err != nil {
		return nil, err
	}

	var endpointInfo *po.EndpointInfo
	endpointInfo, err = _endpointInfoService.Get(pipeline.EndpointId)
	if err != nil {
		return nil, err
	}

	log.Infof("创建[%s]StreamService,SourceInfo: Addr[%s]、User[%s]、Charset[%s]、Flavor[%s]、ServerID[%d]", pipeline.Name, fmt.Sprintf("%s:%d", sourceInfo.Host, sourceInfo.Port), sourceInfo.Username, sourceInfo.Charset, sourceInfo.Flavor, sourceInfo.SlaveID)
	log.Infof("创建[%s]StreamService,EndpointInfo: Type[%s]、Addr[%s]、User[%s]", pipeline.Name, constants.GetEndpointTypeName(endpointInfo.GetType()), endpointInfo.GetAddresses(), endpointInfo.GetUsername())

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

	var rules []*po.TransformRule
	rules, err = _transformRuleService.SelectList(vo.TransformRuleParams{PipelineId: pipeline.Id})
	for _, rule := range rules {
		canalConfig.IncludeTableRegex = append(canalConfig.IncludeTableRegex, rule.Schema+"\\."+rule.Table)
	}

	var conn *canal.Canal
	conn, err = datasource.CreateConnection(sourceInfo)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

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
		var rc *bo.RuleContext
		rc, err = bo.CreateRuleContext(pipeline, rule, tableInfo, true)
		if err != nil {
			break
		}
		ruleContexts[strings.ToLower(rule.Schema+"."+rule.Table)] = rc
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

func closeStreamService(serv *StreamService, cause string) {
	_lockOfStreams.Lock()
	defer _lockOfStreams.Unlock()

	serv.endpointMonitorStopSignal <- struct{}{} //关闭客户端监听
	serv.endpointEnable.Store(false)             //设置客户端不可用状态
	serv.endpointEnable.Store(false)             //设置客户端不可用状态
	serv.endpoint.Close()                        //关闭客户端
	serv.endpoint = nil                          //help GC

	serv.dumper.Close() //关闭dumper
	serv.wg.Wait()
	serv.dumperEnable.Store(false)
	serv.dumper = nil //help GC

	// 关闭所有Lua VM
	for _, rc := range serv.ruleContexts {
		rc.CloseLuaVM()
	}

	serv.runtime.SetStatusCease(cause)
	_streamStateService.SaveCeaseStatus(serv.getPipelineId(), serv.runtime)

	serv.handler.stop() //停止handler
	serv.handler = nil  //help GC

	log.Infof("关闭[%s]StreamService", serv.getPipelineName())
	delete(_streams, serv.getPipelineId())
	serv = nil //help GC
}

func (s *StreamService) startup() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	// 连接接收端
	if err := s.endpoint.Connect(); err != nil {
		return err
	}
	s.endpointEnable.Store(true)

	// 获取Position
	position := _streamStateService.GetPosition(s.getPipelineId())

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
	err = s.runDumper(position)
	if err != nil {
		return err
	}
	s.runtime.SetPosition(position)
	s.runtime.InitStartTime()
	s.startEndpointMonitor()

	return nil
}

func (s *StreamService) runDumper(current mysql.Position) error {
	s.handler.start()
	s.dumperEnable.Store(true)
	s.wg.Add(1)
	go func(current mysql.Position) {
		log.Infof("从Position[%s %d]启动[%s]Dumper", current.Name, current.Pos, s.pipeline.Name)
		if err := s.dumper.RunFrom(current); err != nil {
			msg := fmt.Sprintf("[%s] Dumper启动失败[%s]", s.pipeline.Name, err.Error())
			log.Info(msg)
			s.runtime.SetStatusFault(msg)
			s.handler.stop()
		}
		log.Infof("[%s] Dumper已销毁", s.pipeline.Name)
		s.dumperEnable.Store(false)
		s.dumper = nil
		s.wg.Done()
	}(current)

	time.Sleep(1 * time.Second) //canal未提供回调，停留一秒，确保RunFrom启动成功
	if !s.dumperEnable.Load() {
		return errors.New("启动[%s]Dumper失败")
	}
	log.Infof("[%s] Dumper启动成功", s.pipeline.Name)
	s.runtime.SetStatusRunning() //设置为正常状态
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

	s.runtime.SetStatusFault(cause)
	log.Infof("[%s] StreamService客户端故障", s.getPipelineName())
}

// 客户端恢复
func (s *StreamService) endpointResume() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.runtime.IsRunning() || nil != s.dumper {
		return errors.New("StreamService已启动")
	}

	position := _streamStateService.GetPosition(s.getPipelineId())
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
	err = s.runDumper(position)
	log.Infof("[%s] StreamService恢复运行", s.pipeline.Name)
	return err
}

func (s *StreamService) startEndpointMonitor() {
	go func() {
		log.Infof("[%s] 启动客户端监控", s.getPipelineName())
		ticker := time.NewTicker(_endpointMonitorInterval * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if !s.isEndpointEnable() { //客户端不可用
					err := s.endpoint.Ping() //ping客户端
					if err == nil {          //ping通
						s.endpointEnable.Store(true) //客户端恢复
						if constants.EndpointTypeRabbitMQ == s.endpointType {
							s.endpoint.Connect()
						}
						err = s.endpointResume() //客户端恢复
						if err != nil {
							log.Errorf("[%s] StreamService恢复运行失败[%s]", s.pipeline.Name, err.Error())
						}
					} else {
						log.Errorf("[%s] Ping客户端错误[%s]", s.pipeline.Name, err.Error())
					}
				}
			case <-s.endpointMonitorStopSignal:
				return
			}
		}
	}()
}

func (s *StreamService) addHandleCount(action string, n int) {
	switch action {
	case canal.InsertAction:
		s.runtime.AddInsertCount(n)
	case canal.UpdateAction:
		s.runtime.AddUpdateCount(n)
	case canal.DeleteAction:
		s.runtime.AddDeleteCount(n)
	}
}

func (s *StreamService) getRuleContext(key string) (*bo.RuleContext, bool) {
	rc, exist := s.ruleContexts[key]
	return rc, exist
}

func (s *StreamService) isEndpointEnable() bool {
	return s.endpointEnable.Load()
}

func (s *StreamService) getPipelineName() string {
	return s.pipeline.Name
}

func (s *StreamService) getPipelineId() uint64 {
	return s.pipeline.Id
}

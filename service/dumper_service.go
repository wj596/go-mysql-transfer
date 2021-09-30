package service

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"go.uber.org/atomic"

	"go-mysql-transfer/datasource"
	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/endpoint"
	"go-mysql-transfer/util/log"
)

const _endpointMonitoringInterval = 1

type dumper struct {
	wg               sync.WaitGroup
	lock             sync.Mutex
	monitoringSignal chan struct{}
	startTime        time.Time
	destroyed        atomic.Bool
	pipelineId       uint64
	pipeline         *po.PipelineInfo

	endpoint       endpoint.Endpoint
	endpointEnable atomic.Bool

	dumpDatabases []string
	dumpTables    []string

	canal       *canal.Canal
	canalEnable atomic.Bool
	canalConfig *canal.Config

	handler *eventHandler
}

func newDumper(pipelineId uint64) (*dumper, error) {

	pipeline, err := _pipelineInfoService.Get(pipelineId)
	if err != nil {
		return nil, err
	}

	var sourceInfo *po.SourceInfo
	sourceInfo, err = _sourceInfoService.Get(pipeline.SourceId)
	if err != nil {
		return nil, err
	}

	var endpointInfo *po.EndpointInfo
	endpointInfo, err = _endpointInfoService.Get(pipeline.EndpointId)
	if err != nil {
		return nil, err
	}

	log.Infof("创建Pipeline[%s]实例,SourceInfo: Addr[%s]、User[%s]、Charset[%s]、Flavor[%s]、ServerID[%d]", pipeline.Name, fmt.Sprintf("%s:%d", sourceInfo.Host, sourceInfo.Port), sourceInfo.Username, sourceInfo.Charset, sourceInfo.Flavor, sourceInfo.SlaveID)
	log.Infof("创建Pipeline[%s]实例,EndpointInfo: Type[%s]、Addr[%s]、User[%s]", pipeline.Name, constants.GetEndpointTypeName(endpointInfo.GetType()), endpointInfo.GetAddresses(), endpointInfo.GetUsername())

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
	rules, err = _transformRuleService.SelectList(pipelineId, 0)
	for _, rule := range rules {
		canalConfig.IncludeTableRegex = append(canalConfig.IncludeTableRegex, rule.Schema+"\\."+rule.Table)
	}

	var ds *canal.Canal
	ds, err = datasource.CreateCanal(sourceInfo)
	if err != nil {
		return nil, err
	}
	defer ds.Close()

	schemas := make(map[string]bool)
	tables := make([]string, 0, len(rules))
	for _, rule := range rules {
		tableInfo, err := ds.GetTable(rule.Schema, rule.Table)
		if err != nil {
			return nil, err
		}
		schemas[rule.Schema] = true
		tables = append(tables, rule.Table)
		key := strings.ToLower(strconv.FormatUint(pipeline.Id, 10) + "." + rule.Schema + "." + rule.Table)
		bo.RuntimeRules[key] = bo.NewRuntimeRule(rule, tableInfo)
	}

	dumpDatabases := make([]string, 0, len(schemas))
	for k, _ := range schemas {
		dumpDatabases = append(dumpDatabases, k)
	}

	ins := &dumper{
		startTime:        time.Now(),
		pipelineId:       pipelineId,
		pipeline:         pipeline,
		endpoint:         endpoint.NewEndpoint(endpointInfo),
		monitoringSignal: make(chan struct{}, 1),
		dumpDatabases:    dumpDatabases,
		dumpTables:       dumpDatabases,
		canalConfig:      canalConfig,
	}

	return ins, nil
}

func (s *dumper) start() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.isCanalEnable() { //Canal启动
		return errors.New("Pipeline[%s]已经启动")
	}

	// 连接接收端
	if err := s.endpoint.Connect(); err != nil {
		return errors.Errorf("无法启动，接收端故障，原因：%s", err.Error())
	}
	s.setEndpointEnable(true)

	position, err := _pipelineInfoService.GetPosition(s.pipelineId)
	if err != nil {
		return err
	}

	canal, err := canal.NewCanal(s.canalConfig)
	if err != nil {
		return err
	}
	if len(s.dumpDatabases) == 1 {
		canal.AddDumpTables(s.dumpDatabases[0], s.dumpTables...)
	} else {
		canal.AddDumpDatabases(s.dumpDatabases...)
	}

	handler := newEventHandler(s)
	canal.SetEventHandler(handler)
	s.handler = handler
	s.canal = canal
	s.run(position)
	s.startMonitoring()

	return nil
}

func (s *dumper) restart() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.isCanalEnable() { //Canal启动
		return errors.New("Pipeline[%s]已经启动")
	}

	if nil != s.canal { //Canal启动
		return errors.New("Pipeline[%s]已经启动")
	}

	position, err := _pipelineInfoService.GetPosition(s.pipelineId)
	if err != nil {
		return err
	}

	canal, err := canal.NewCanal(s.canalConfig)
	if err != nil {
		return err
	}
	if len(s.dumpDatabases) == 1 {
		canal.AddDumpTables(s.dumpDatabases[0], s.dumpTables...)
	} else {
		canal.AddDumpDatabases(s.dumpDatabases...)
	}
	log.Infof("Pipeline[%s]重启", s.pipeline.Name)
	handler := newEventHandler(s)
	canal.SetEventHandler(handler)
	s.handler = handler
	s.canal = canal
	s.run(position)

	return nil
}

func (s *dumper) run(current mysql.Position) {
	s.wg.Add(1)
	s.handler.start()
	go func(current mysql.Position) {
		log.Infof("启动Pipeline[%s]，From Position[%s %d]", s.pipeline.Name, current.Name, current.Pos)
		if err := s.canal.RunFrom(current); err != nil {
			log.Infof(fmt.Sprintf("启动Pipeline[%s]失败，原因[%s]", s.pipeline.Name, err.Error()))
			s.handler.stop()
		}
		log.Infof("Pipeline[%s]，Canal已经关闭", s.pipeline.Name)
		s.setCanalEnable(false)
		s.canal = nil
		s.wg.Done()
	}(current)
	time.Sleep(time.Second) //canal未提供回调，停留一秒，确保RunFrom启动成功
	s.setCanalEnable(true)
}

func (s *dumper) stop() {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.canal == nil || !s.isCanalEnable() {
		return
	}

	s.canal.Close()
	s.wg.Wait()
	s.setCanalEnable(false)
	s.canal = nil

	s.handler.stop()
	s.handler = nil

	log.Infof("Pipeline[%s] Canal Stopped", s.pipeline.Name)
}

func (s *dumper) destroy() {
	s.setDestroyed(true)
	s.stopMonitoring()
	s.stop()
	s.endpoint.Close()
	s.setEndpointEnable(false)
}

func (s *dumper) startMonitoring() {
	go func() {
		log.Infof("Pipeline[%s]实例，启动Endpoint Monitoring", s.pipeline.Name)
		ticker := time.NewTicker(_endpointMonitoringInterval * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if !s.isEndpointEnable() {
					err := s.endpoint.Ping()
					if err == nil {
						s.endpointEnable.Store(true)
						//if global.Cfg().IsRabbitmq() {
						//	s.endpoint.Connect()
						//}
						s.restart()
						//metrics.SetDestState(metrics.DestStateOK)
					} else {
						log.Errorf("Pipeline[%s] endpoint ping error[%s]", s.pipeline.Name, err.Error())
					}
				}
			case <-s.monitoringSignal:
				return
			}
		}
	}()
}

func (s *dumper) stopMonitoring() {
	log.Infof("Pipeline[%s] Stop Monitoring", s.pipeline.Name)
	s.monitoringSignal <- struct{}{}
}

func (s *dumper) isEndpointEnable() bool {
	return s.endpointEnable.Load()
}

func (s *dumper) isCanalEnable() bool {
	return s.canalEnable.Load()
}

func (s *dumper) setCanalEnable(status bool) {
	s.canalEnable.Store(status)
}

func (s *dumper) setEndpointEnable(status bool) {
	s.endpointEnable.Store(status)
}

func (s *dumper) isDestroyed() bool {
	return s.destroyed.Load()
}

func (s *dumper) setDestroyed(status bool) {
	s.destroyed.Store(status)
}

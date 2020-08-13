package service

import (
	"context"
	"fmt"
	"go.uber.org/atomic"

	"regexp"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"

	"go-mysql-transfer/global"
	"go-mysql-transfer/service/endpoint"
	"go-mysql-transfer/storage"
	"go-mysql-transfer/util/logutil"
)

const _metricsTaskInterval = 10

type TransferService struct {
	config          *global.Config
	canal           *canal.Canal
	positionStorage storage.PositionStorage

	endpoint endpoint.Endpoint
	handler  *handler

	listenerStarted atomic.Bool
	running         atomic.Bool
	wg              sync.WaitGroup
	ctx             context.Context
	cancelFunc      context.CancelFunc
}

func (s *TransferService) initialize() error {
	if err := s.initCanal(); err != nil {
		return errors.Trace(err)
	}

	if err := s.initRules(); err != nil {
		return errors.Trace(err)
	}

	// 初始化 endpoint
	_endpoint := endpoint.NewEndpoint(s.config)
	if err := _endpoint.Start(); err != nil {
		return errors.Trace(err)
	}
	global.SetDestinationState(global.MetricsStateOK)
	s.endpoint = _endpoint

	s.initDumper()

	s.positionStorage = storage.NewPositionStorage(s.config)
	if err := s.positionStorage.Initialize(); err != nil {
		return errors.Trace(err)
	}

	s.ctx, s.cancelFunc = context.WithCancel(context.Background())

	s.handler = &handler{
		requestQueue: make(chan interface{}, 4096),
		transfer:     s,
	}
	s.canal.SetEventHandler(s.handler)

	return nil
}

func (s *TransferService) run() error {
	s.wg.Add(1)
	s.handler.startRequestQueueListener()
	s.startMetricsTask()
	s.endpoint.StartRetryTask()

	current, err := s.positionStorage.Get()
	if err != nil {
		return err
	}

	logutil.BothInfof("transfer run from pos %s %d", current.Name, current.Pos)

	s.running.Store(true)
	if err := s.canal.RunFrom(current); err != nil {
		logutil.Errorf("start transfer err %v", err)
		return errors.Trace(err)
	}

	s.running.Store(false)
	logutil.Info("Canal is Closed")
	return nil
}

func (s *TransferService) CtxDone() <-chan struct{} {
	return s.ctx.Done()
}

func (s *TransferService) CtxErr() error {
	return s.ctx.Err()
}

func (s *TransferService) Pause() {
	if s.running.Load() {
		logutil.BothInfof("transfer paused !!!")
		s.canal.Close()
		s.canal = nil
		s.running.Store(false)
	}
}

func (s *TransferService) Restart() {
	if s.listenerStarted.Load() {
		if s.canal == nil {
			logutil.BothInfof("transfer rerun !!!")
			go s.rerun()
		}
	} else {
		logutil.BothInfof("transfer run !!!")
		go s.run()
	}
}

func (s *TransferService) rerun() {
	s.initCanal()
	s.initDumper()
	s.canal.SetEventHandler(s.handler)
	s.running.Store(true)

	current, _ := s.positionStorage.Get()
	logutil.Infof("TransferService Restart! ,Position: %s-%d", current.Name, current.Pos)
	if err := s.canal.RunFrom(current); err != nil {
		logutil.Errorf("start transfer err %v", err)
	}
}

func (s *TransferService) close() {
	logutil.Infof("closing transfer")

	s.cancelFunc()

	if s.canal != nil {
		s.canal.Close()
	}

	s.endpoint.Close()

	s.wg.Wait()
}

func (s *TransferService) initCanal() error {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = s.config.Addr
	cfg.User = s.config.User
	cfg.Password = s.config.Password
	cfg.Charset = s.config.Charset
	cfg.Flavor = s.config.Flavor
	cfg.ServerID = s.config.SlaveID
	cfg.Dump.ExecutionPath = s.config.DumpExec
	cfg.Dump.DiscardErr = false
	cfg.Dump.SkipMasterData = s.config.SkipMasterData

	for _, s := range s.config.RuleConfigs {
		cfg.IncludeTableRegex = append(cfg.IncludeTableRegex, s.Schema+"\\."+s.Table)
	}

	var err error
	s.canal, err = canal.NewCanal(cfg)
	return errors.Trace(err)
}

func (s *TransferService) initRules() error {
	wildcards := make(map[string]bool)
	for _, rc := range s.config.RuleConfigs {
		if rc.Table == "*" {
			return errors.Errorf("wildcard * is not allowed for table name")
		}

		if regexp.QuoteMeta(rc.Table) != rc.Table { //通配符
			if _, ok := wildcards[global.RuleKey(rc.Schema, rc.Schema)]; ok {
				return errors.Errorf("duplicate wildcard table defined for %s.%s", rc.Schema, rc.Table)
			}

			tableName := rc.Table
			if rc.Table == "*" {
				tableName = "." + rc.Table
			}
			sql := fmt.Sprintf(`SELECT table_name FROM information_schema.tables WHERE
					table_name RLIKE "%s" AND table_schema = "%s";`, tableName, rc.Schema)
			res, err := s.canal.Execute(sql)
			if err != nil {
				return errors.Trace(err)
			}
			for i := 0; i < res.Resultset.RowNumber(); i++ {
				tableName, _ := res.GetString(i, 0)
				newRule, err := global.RuleDeepClone(rc)
				if err != nil {
					return errors.Trace(err)
				}
				newRule.Table = tableName
				ruleKey := global.RuleKey(rc.Schema, tableName)
				global.AddRuleIns(ruleKey, newRule)
			}
		} else {
			newRule, err := global.RuleDeepClone(rc)
			if err != nil {
				return errors.Trace(err)
			}
			ruleKey := global.RuleKey(rc.Schema, rc.Table)
			global.AddRuleIns(ruleKey, newRule)
		}
	}

	for _, rule := range global.RuleInsList() {
		tableMata, err := s.canal.GetTable(rule.Schema, rule.Table)
		if err != nil {
			return errors.Trace(err)
		}
		if len(tableMata.PKColumns) == 0 {
			if !s.config.SkipNoPkTable {
				return errors.Errorf("%s.%s must have a PK for a column", rule.Schema, rule.Table)
			}
		}
		if len(tableMata.PKColumns) > 1 {
			rule.CompositeKey = true // 组合主键
		}
		rule.TableInfo = tableMata

		if err := rule.Initialize(); err != nil {
			return errors.Trace(err)
		}

		if rule.LuaNecessary() {
			if err := rule.PreCompileLuaScript(s.config.DataDir); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *TransferService) initDumper() {
	var schema string
	schemas := make(map[string]int)
	tables := make([]string, 0, global.RuleInsTotal())
	for _, rule := range global.RuleInsList() {
		schema = rule.Table
		schemas[rule.Schema] = 1
		tables = append(tables, rule.Table)
	}
	if len(schemas) == 1 {
		s.canal.AddDumpTables(schema, tables...)
	} else {
		keys := make([]string, 0, len(schemas))
		for key := range schemas {
			keys = append(keys, key)
		}
		s.canal.AddDumpDatabases(keys...)
	}
}

func (s *TransferService) updateRule(schema, table string) error {
	rule, ok := global.RuleIns(global.RuleKey(schema, table))
	if ok {
		tableInfo, err := s.canal.GetTable(schema, table)
		if err != nil {
			return errors.Trace(err)
		}

		if len(tableInfo.PKColumns) == 0 {
			if !s.config.SkipNoPkTable {
				return errors.Errorf("%s.%s must have a PK for a column", rule.Schema, rule.Table)
			}
		}

		if len(tableInfo.PKColumns) > 1 {
			rule.CompositeKey = true
		}

		rule.TableInfo = tableInfo

		err = rule.AfterUpdateTableInfo()
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *TransferService) startMetricsTask() {
	ticker := time.NewTicker(_metricsTaskInterval * time.Second)
	go func() {
		for {
			<-ticker.C
			if err := s.endpoint.Ping(); err != nil {
				global.SetDestinationState(global.MetricsStateNO)
			} else {
				global.SetDestinationState(global.MetricsStateOK)
			}

			global.SetTransferDelay(s.canal.GetDelay())
		}
	}()
}

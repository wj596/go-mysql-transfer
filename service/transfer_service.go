/*
 * Copyright 2020-2021 the original author(https://github.com/wj596)
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
	"log"
	"regexp"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"go.uber.org/atomic"

	"go-mysql-transfer/global"
	"go-mysql-transfer/metrics"
	"go-mysql-transfer/service/endpoint"
	"go-mysql-transfer/storage"
	"go-mysql-transfer/util/logs"
)

const _transferLoopInterval = 1

type TransferService struct {
	canal        *canal.Canal
	canalCfg     *canal.Config
	canalHandler *handler
	canalEnable  atomic.Bool
	lockOfCanal  sync.Mutex
	firstsStart  atomic.Bool

	wg             sync.WaitGroup
	endpoint       endpoint.Endpoint
	endpointEnable atomic.Bool
	positionDao    storage.PositionStorage
	loopStopSignal chan struct{}
}

func (s *TransferService) initialize() error {
	s.canalCfg = canal.NewDefaultConfig()
	s.canalCfg.Addr = global.Cfg().Addr
	s.canalCfg.User = global.Cfg().User
	s.canalCfg.Password = global.Cfg().Password
	s.canalCfg.Charset = global.Cfg().Charset
	s.canalCfg.Flavor = global.Cfg().Flavor
	s.canalCfg.ServerID = global.Cfg().SlaveID
	s.canalCfg.Dump.ExecutionPath = global.Cfg().DumpExec
	s.canalCfg.Dump.DiscardErr = false
	s.canalCfg.Dump.SkipMasterData = global.Cfg().SkipMasterData

	if err := s.createCanal(); err != nil {
		return errors.Trace(err)
	}

	if err := s.completeRules(); err != nil {
		return errors.Trace(err)
	}

	s.addDumpDatabaseOrTable()

	positionDao := storage.NewPositionStorage()
	if err := positionDao.Initialize(); err != nil {
		return errors.Trace(err)
	}
	s.positionDao = positionDao

	// endpoint
	endpoint := endpoint.NewEndpoint(s.canal)
	if err := endpoint.Connect(); err != nil {
		return errors.Trace(err)
	}
	// 异步，必须要ping下才能确定连接成功
	if global.Cfg().IsMongodb() {
		err := endpoint.Ping()
		if err != nil {
			return err
		}
	}
	s.endpoint = endpoint
	s.endpointEnable.Store(true)
	metrics.SetDestState(metrics.DestStateOK)

	s.firstsStart.Store(true)
	s.startLoop()

	return nil
}

func (s *TransferService) run() error {
	current, err := s.positionDao.Get()
	if err != nil {
		return err
	}

	s.wg.Add(1)
	go func(p mysql.Position) {
		s.canalEnable.Store(true)
		log.Println(fmt.Sprintf("transfer run from position(%s %d)", p.Name, p.Pos))
		if err := s.canal.RunFrom(p); err != nil {
			log.Println(fmt.Sprintf("start transfer : %v", err))
			logs.Errorf("canal : %v", errors.ErrorStack(err))
			if s.canalHandler != nil {
				s.canalHandler.stopListener()
			}
			s.canalEnable.Store(false)
		}

		logs.Info("Canal is Closed")
		s.canalEnable.Store(false)
		s.canal = nil
		s.wg.Done()
	}(current)

	// canal未提供回调，停留一秒，确保RunFrom启动成功
	time.Sleep(time.Second)
	return nil
}

func (s *TransferService) StartUp() {
	s.lockOfCanal.Lock()
	defer s.lockOfCanal.Unlock()

	if s.firstsStart.Load() {
		s.canalHandler = newHandler()
		s.canal.SetEventHandler(s.canalHandler)
		s.canalHandler.startListener()
		s.firstsStart.Store(false)
		s.run()
	} else {
		s.restart()
	}
}

func (s *TransferService) restart() {
	if s.canal != nil {
		s.canal.Close()
		s.wg.Wait()
	}

	s.createCanal()
	s.addDumpDatabaseOrTable()
	s.canalHandler = newHandler()
	s.canal.SetEventHandler(s.canalHandler)
	s.canalHandler.startListener()
	s.run()
}

func (s *TransferService) stopDump() {
	s.lockOfCanal.Lock()
	defer s.lockOfCanal.Unlock()

	if s.canal == nil {
		return
	}

	if !s.canalEnable.Load() {
		return
	}

	if s.canalHandler != nil {
		s.canalHandler.stopListener()
		s.canalHandler = nil
	}

	s.canal.Close()
	s.wg.Wait()

	log.Println("dumper stopped")
}

func (s *TransferService) Close() {
	s.stopDump()
	s.loopStopSignal <- struct{}{}
}

func (s *TransferService) Position() (mysql.Position, error) {
	return s.positionDao.Get()
}

func (s *TransferService) createCanal() error {
	for _, rc := range global.Cfg().RuleConfigs {
		s.canalCfg.IncludeTableRegex = append(s.canalCfg.IncludeTableRegex, rc.Schema+"\\."+rc.Table)
	}
	var err error
	s.canal, err = canal.NewCanal(s.canalCfg)
	return errors.Trace(err)
}

func (s *TransferService) completeRules() error {
	wildcards := make(map[string]bool)
	for _, rc := range global.Cfg().RuleConfigs {
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
			if !global.Cfg().SkipNoPkTable {
				return errors.Errorf("%s.%s must have a PK for a column", rule.Schema, rule.Table)
			}
		}
		if len(tableMata.PKColumns) > 1 {
			rule.IsCompositeKey = true // 组合主键
		}
		rule.TableInfo = tableMata
		rule.TableColumnSize = len(tableMata.Columns)

		if err := rule.Initialize(); err != nil {
			return errors.Trace(err)
		}

		if rule.LuaEnable() {
			if err := rule.CompileLuaScript(global.Cfg().DataDir); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *TransferService) addDumpDatabaseOrTable() {
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
			if !global.Cfg().SkipNoPkTable {
				return errors.Errorf("%s.%s must have a PK for a column", rule.Schema, rule.Table)
			}
		}

		if len(tableInfo.PKColumns) > 1 {
			rule.IsCompositeKey = true
		}

		rule.TableInfo = tableInfo
		rule.TableColumnSize = len(tableInfo.Columns)

		err = rule.AfterUpdateTableInfo()
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *TransferService) startLoop() {
	go func() {
		ticker := time.NewTicker(_transferLoopInterval * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if !s.endpointEnable.Load() {
					err := s.endpoint.Ping()
					if err != nil {
						log.Println("destination not available,see the log file for details")
						logs.Error(err.Error())
					} else {
						s.endpointEnable.Store(true)
						if global.Cfg().IsRabbitmq() {
							s.endpoint.Connect()
						}
						s.StartUp()
						metrics.SetDestState(metrics.DestStateOK)
					}
				}
			case <-s.loopStopSignal:
				return
			}
		}
	}()
}

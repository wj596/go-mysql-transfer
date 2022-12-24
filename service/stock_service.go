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
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"go.uber.org/atomic"
	"log"
	"regexp"
	"strings"
	"sync"

	"go-mysql-transfer/global"
	"go-mysql-transfer/model"
	"go-mysql-transfer/service/endpoint"
	"go-mysql-transfer/util/dates"
	"go-mysql-transfer/util/logs"
)

// 存量数据
type StockService struct {
	canal    *canal.Canal
	endpoint endpoint.Endpoint

	queueCh       chan []*model.RowRequest
	counter       map[string]int64
	lockOfCounter sync.Mutex
	totalRows     map[string]int64
	wg            sync.WaitGroup
	shutoff       *atomic.Bool
}

func NewStockService() *StockService {
	return &StockService{
		queueCh:   make(chan []*model.RowRequest, global.Cfg().Maxprocs),
		counter:   make(map[string]int64),
		totalRows: make(map[string]int64),
		shutoff:   atomic.NewBool(false),
	}
}

type Task struct {
	fullName string
	sql string
	rule *global.Rule
}

type Result struct{
	fullName string
	count int64
}


func (s *StockService) Run() error {
	canalCfg := canal.NewDefaultConfig()
	canalCfg.Addr = global.Cfg().Addr
	canalCfg.User = global.Cfg().User
	canalCfg.Password = global.Cfg().Password
	canalCfg.Charset = global.Cfg().Charset
	canalCfg.Flavor = global.Cfg().Flavor
	canalCfg.ServerID = global.Cfg().SlaveID
	canalCfg.Dump.ExecutionPath = global.Cfg().DumpExec
	canalCfg.Dump.DiscardErr = false
	canalCfg.Dump.SkipMasterData = global.Cfg().SkipMasterData

	if c, err := canal.NewCanal(canalCfg); err != nil {
		errors.Trace(err)
	} else {
		s.canal = c
	}

	if err := s.completeRules(); err != nil {
		return errors.Trace(err)
	}
	s.addDumpDatabaseOrTable()

	endpoint := endpoint.NewEndpoint(s.canal)
	if err := endpoint.Connect(); err != nil {
		log.Println(err.Error())
		return errors.Trace(err)
	}
	s.endpoint = endpoint

	startTime := dates.NowMillisecond()
	log.Println(fmt.Sprintf("bulk size: %d", global.Cfg().BulkSize))


	taskChan := make(chan *Task,0)
	resultChan := make(chan *Result,0)
	dones := make(chan bool, global.Cfg().Maxprocs)
	//开启worker池
	for i := 0; i < global.Cfg().Maxprocs; i++ {
		go func(wid int,taskChan chan *Task,resultChan chan *Result,dones chan bool){
			defer func(){
				dones <- true
			}()
			for task := range taskChan{
				requests, err := s.export(task)
				if err != nil {
					logs.Error(err.Error())
					s.shutoff.Store(true)
					return
				}
				count := s.imports(task.fullName, requests)
				resultChan <- &Result{
					fullName:task.fullName,
					count: count,
				}
			}


		}(i,taskChan,resultChan,dones)
	}

	go func() error{
		defer func() {
			close(taskChan)
		}()
		for _, rule := range global.RuleInsList() {
			if rule.OrderByColumn == "" {
				return errors.New("empty order_by_column not allowed")
			}
			exportColumns := s.exportColumns(rule)
			fullName := fmt.Sprintf("%s.%s", rule.Schema, rule.Table)
			log.Println(fmt.Sprintf("开始导出 %s", fullName))
			i := rule.TableInfo.PKColumns[0]
			pk := rule.TableInfo.GetPKColumn(i).Name
			//这里写死 主键为自增id
			minSql := fmt.Sprintf("select min(%s) as min_id from %s", pk, fullName)
			log.Println(fmt.Sprintf("minSql %s", minSql))
			maxSql := fmt.Sprintf("select max(%s) as max_id from %s", pk, fullName)
			log.Println(fmt.Sprintf("maxSql %s", maxSql))
			minRes, err := s.canal.Execute(minSql)
			if err != nil {
				return err
			}
			minId, err := minRes.GetIntByName(0, "min_id")
			if err != nil {
				return err
			}

			maxRes, err := s.canal.Execute(maxSql)
			if err != nil {
				return err
			}
			maxId, err := maxRes.GetIntByName(0, "max_id")
			if err != nil {
				return err
			}
			log.Println(fmt.Sprintf("minId:%d,maxId,%d", minId, maxId))
			start := minId - 1
			end := maxId
			//res, err := s.canal.Execute(fmt.Sprintf("select count(1) from %s", fullName))
			//if err != nil {
			//	return err
			//}
			//log.Println(fmt.Sprintf("res:%v", res))
			//totalRow, err := res.GetInt(0, 0)
			//s.totalRows[fullName] = totalRow
			//log.Println(fmt.Sprintf("%s 共 %d 条数据", fullName, totalRow))
			for start < end {
				_end := start + global.Cfg().BulkSize
				if _end >= end {
					_end = end
				}
				sql := s.buildSql(fullName, exportColumns, start, _end, rule)
				task := &Task{
					fullName: fullName,
					sql:      sql,
					rule:     rule,
				}
				taskChan <- task
				start = _end
			}
		}
		return nil
	}()

	go func(){
		for i := 0; i < global.Cfg().Maxprocs; i++ {
			<- dones
		}
		close(resultChan)
	}()
	for r := range resultChan{
		//fmt.Println(fmt.Sprintf("表： %s，导入：%d 条", r.fullName, r.count))
		if _,ok := s.counter[r.fullName];ok{
			s.counter[r.fullName] += r.count
		}else{
			s.counter[r.fullName]  = r.count
		}

	}


	log.Println(fmt.Sprintf("共耗时 ：%d（毫秒）", dates.NowMillisecond()-startTime))

	for k, v := range s.counter {
		log.Printf("表:%s 共导入 %d条====",k,v)
	}

	s.endpoint.Close() // 关闭客户端

	return nil
}

func (s *StockService) export(task *Task) ([]*model.RowRequest, error) {
	if s.shutoff.Load() {
		return nil, errors.New("shutoff")
	}
	sql := task.sql
	rule := task.rule
	resultSet, err := s.canal.Execute(sql)
	if err != nil {
		logs.Errorf("数据导出错误: %s - %s", sql, err.Error())
		return nil, err
	}
	rowNumber := resultSet.RowNumber()
	requests := make([]*model.RowRequest, 0, rowNumber)
	for i := 0; i < rowNumber; i++ {
		rowValues := make([]interface{}, 0, len(rule.TableInfo.Columns))
		request := new(model.RowRequest)
		for j := 0; j < len(rule.TableInfo.Columns); j++ {
			val, err := resultSet.GetValue(i, j)
			if err != nil {
				logs.Errorf("数据导出错误: %s - %s", sql, err.Error())
				break
			}
			rowValues = append(rowValues, val)
			request.Action = canal.InsertAction
			request.RuleKey = global.RuleKey(rule.Schema, rule.Table)
			request.Row = rowValues
		}
		requests = append(requests, request)
	}

	return requests, nil
}

// 构造SQL
func (s *StockService) buildSql(fullName, columns string, start int64,end int64, rule *global.Rule) string {
	i := rule.TableInfo.PKColumns[0]
	pk := rule.TableInfo.GetPKColumn(i).Name
	t := "select * from %s where %s>%d and %s <= %d"
	sql := fmt.Sprintf(t, fullName, pk, start, pk, end)
	return sql
}

func (s *StockService) imports(fullName string, requests []*model.RowRequest)int64 {
	if s.shutoff.Load() {
		return 0
	}
	count := s.endpoint.Stock(requests)
	log.Println(fmt.Sprintf("%s 导入数据 %d 条", fullName, count))
	return count
}

func (s *StockService) exportColumns(rule *global.Rule) string {
	if rule.IncludeColumnConfig != "" {
		var columns string
		includes := strings.Split(rule.IncludeColumnConfig, ",")
		for _, c := range rule.TableInfo.Columns {
			for _, e := range includes {
				var column string
				if strings.ToUpper(e) == strings.ToUpper(c.Name) {
					column = c.Name
				} else {
					column = "null as " + c.Name
				}

				if columns != "" {
					columns = columns + ","
				}
				columns = columns + column
			}
		}
		return columns
	}

	if rule.ExcludeColumnConfig != "" {
		var columns string
		excludes := strings.Split(rule.ExcludeColumnConfig, ",")
		for _, c := range rule.TableInfo.Columns {
			for _, e := range excludes {
				var column string
				if strings.ToUpper(e) == strings.ToUpper(c.Name) {
					column = "null as " + c.Name
				} else {
					column = c.Name
				}

				if columns != "" {
					columns = columns + ","
				}
				columns = columns + column
			}
		}
		return columns
	}

	return "*"
}

func (s *StockService) offset(currentPage int64) int64 {
	var offset int64

	if currentPage > 0 {
		offset = (currentPage - 1) * global.Cfg().BulkSize
	}

	return offset
}

func (s *StockService) Close() {
	s.canal.Close()
}

func (s *StockService) incCounter(name string, n int64) int64 {
	s.lockOfCounter.Lock()
	defer s.lockOfCounter.Unlock()

	c, ok := s.counter[name]
	if ok {
		c = c + n
		s.counter[name] = c
	}

	return c
}

func (s *StockService) completeRules() error {
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

func (s *StockService) addDumpDatabaseOrTable() {
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

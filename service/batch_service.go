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
	"strconv"
	"strings"
	"sync"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/schema"
	"github.com/yuin/gopher-lua"
	"go.uber.org/atomic"

	"go-mysql-transfer/datasource"
	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/endpoint"
	"go-mysql-transfer/endpoint/luaengine"
	"go-mysql-transfer/util/commons"
	"go-mysql-transfer/util/log"
)

var (
	_lockOfBatchService sync.Mutex
	_selectCountSql     = "select count(1) from %s"
)

type BatchService struct {
	runtime        *bo.PipelineRuntime
	pipeline       *po.PipelineInfo
	endpoint       endpoint.IBatchEndpoint
	ruleContexts   map[string]*bo.RuleContext
	connectionPool *datasource.ConnectionPool
	dataSourceName string
	bulkSize       int64
	coroutines     int
	statements     map[string]string
	luaVMs         map[string]*lua.LState
	lockOfLuaVMs   sync.Mutex
	wg             sync.WaitGroup
	shutoff        *atomic.Bool
}

func createBatchService(sourceInfo *po.SourceInfo, endpointInfo *po.EndpointInfo, pipeline *po.PipelineInfo, runtime *bo.PipelineRuntime) (*BatchService, error) {
	_lockOfBatchService.Lock()
	defer _lockOfBatchService.Unlock()

	coroutines := int(pipeline.BatchCoroutines)
	if coroutines <= 0 {
		coroutines = constants.BatchCoroutines
	}

	connectionPool, err := datasource.NewConnectionPool(coroutines, sourceInfo)
	if err != nil {
		return nil, err
	}

	counters := make(map[string]*atomic.Uint64)
	totals := make(map[string]*atomic.Uint64)
	contexts := make(map[string]*bo.RuleContext)
	statements := make(map[string]string)
	for _, rule := range pipeline.Rules {
		var tableInfo *schema.Table
		tableInfo, err = connectionPool.Get().GetTable(rule.Schema, rule.Table)
		if err != nil {
			break
		}

		var context *bo.RuleContext
		context, err = bo.CreateRuleContext(pipeline, rule, tableInfo)
		if err != nil {
			break
		}
		contexts[context.GetTableFullName()] = context
		statements[context.GetTableFullName()] = buildStatement(context)
		totals[context.GetTableFullName()] = atomic.NewUint64(0)
		counters[context.GetTableFullName()] = atomic.NewUint64(0)
	}

	if err != nil {
		connectionPool.Shutdown() //关闭连接池
		return nil, err
	}

	runtime.BatchTotalCounters = totals
	runtime.BatchInsertCounters = counters

	bulkSize := int64(pipeline.BatchBulkSize)
	if bulkSize <= 0 {
		bulkSize = int64(constants.BatchBulkSize)
	}

	batchService := &BatchService{
		shutoff:        atomic.NewBool(false),
		dataSourceName: commons.GetDataSourceName(sourceInfo.GetUsername(), sourceInfo.GetPassword(), sourceInfo.GetHost(), "%s", sourceInfo.GetPort(), sourceInfo.GetCharset()),
		endpoint:       endpoint.NewBatchEndpoint(endpointInfo),
		ruleContexts:   contexts,
		connectionPool: connectionPool,
		statements:     statements,
		runtime:        runtime,
		bulkSize:       bulkSize,
		coroutines:     coroutines,
		pipeline:       pipeline,
	}

	return batchService, err
}

// 构造SQL
func buildStatement(ctx *bo.RuleContext) string {
	orderColumn := ctx.GetRule().GetOrderColumn()
	tableFullName := ctx.GetTableFullName()
	tableInfo := ctx.GetTableInfo()
	if len(tableInfo.PKColumns) == 1 {
		i := tableInfo.PKColumns[0]
		pk := tableInfo.GetPKColumn(i).Name
		includes := getSelectColumns("b", ctx)
		return "select " + includes + " from (select " + pk + " from " + tableFullName + " order by " + orderColumn + " limit %d, %d) a left join " + tableFullName + " b on a." + pk + "=b." + pk
	} else {
		includes := getSelectColumns("", ctx)
		return "select " + includes + " from " + tableFullName + " order by " + orderColumn + " limit %d, %d"
	}
}

func getSelectColumns(prefix string, ctx *bo.RuleContext) string {
	var includes string
	for _, column := range ctx.GetTableInfo().Columns {
		if includes != "" {
			includes = includes + ","
		}
		var name string
		if prefix != "" {
			name = prefix + "." + column.Name
		} else {
			name = column.Name
		}
		includes = includes + name
	}
	return includes
}

func (s *BatchService) startup() error {
	defer s.Shutdown()

	_stateService.updateStateByBatching(s.pipeline.Id, s.runtime)

	if err := s.endpoint.Connect(); err != nil {
		log.Errorf(err.Error())
		return errors.Trace(err)
	}

	for _, ctx := range s.ruleContexts {
		tableFullName := ctx.GetTableFullName()

		if ctx.GetRule().GetOrderColumn() == "" {
			return errors.Errorf("BatchService[%s], 表[%s]排序列不能为空", ctx.GetPipelineName(), tableFullName)
		}

		res, err := s.connectionPool.Get().Execute(fmt.Sprintf(_selectCountSql, tableFullName))
		if err != nil {
			return err
		}
		var totalRow int64
		totalRow, err = res.GetInt(0, 0)
		res.Close()
		if err != nil {
			return err
		}

		totalCounter := s.runtime.GetBatchTotalCounter(tableFullName)
		totalCounter.Store(uint64(totalRow))

		var batch int64
		if totalRow%s.bulkSize == 0 {
			batch = totalRow / s.bulkSize
		} else {
			batch = (totalRow / s.bulkSize) + 1
		}
		log.Infof("BatchService[%s] 开始导出表[%s]，共[%d]条，[%d]批，每批[%d]条", ctx.GetPipelineName(), tableFullName, totalRow, batch, s.bulkSize)

		var processed atomic.Int64
		insertCounter := s.runtime.GetBatchInsertCounter(tableFullName)
		for i := 0; i < s.coroutines; i++ {
			var lvm *lua.LState
			if ctx.IsLuaEnable() {
				key := tableFullName + "-" + strconv.Itoa(i)
				lvm, err = s.getOrCreateLuaVM(key, ctx)
				if err != nil {
					return err
				}
			}
			s.wg.Add(1)
			go func(_ctx *bo.RuleContext, _lvm *lua.LState, _counter *atomic.Uint64) {
				for {
					processed.Inc()
					var requests []*bo.RowEventRequest
					requests, err = s.export(processed.Load(), _ctx)
					if err != nil {
						log.Error(err.Error())
						s.shutoff.Store(true)
						break
					}

					err = s.imports(requests, _ctx, _lvm, _counter)
					if err != nil {
						log.Error(err.Error())
						s.shutoff.Store(true)
						break
					}

					if processed.Load() > batch {
						break
					}
				}
				s.wg.Done()
			}(ctx, lvm, insertCounter)
		}
	}

	s.wg.Wait()

	for k, v := range s.runtime.BatchTotalCounters {
		vv := s.runtime.GetBatchInsertCounter(k)
		if v.Load() > vv.Load() {
			s.runtime.LatestMessage.Store("存在导入错误的数据，具体请至日志查看")
		}
	}

	return nil
}

func (s *BatchService) Shutdown() {
	_stateService.updateStateByBatchEnd(s.pipeline.Id, s.runtime)

	if nil != s.endpoint {
		s.endpoint.Close() // 关闭客户端
	}

	s.connectionPool.Shutdown() // 关闭数据库连接池

	for _, vm := range s.luaVMs { // 关闭LUA虚拟机
		vm.Close()
	}

	// 告警
	_alarmService.batchReport(s.pipeline, s.runtime)
}

func (s *BatchService) getOrCreateLuaVM(key string, ctx *bo.RuleContext) (*lua.LState, error) {
	s.lockOfLuaVMs.Lock()
	defer s.lockOfLuaVMs.Unlock()

	log.Infof("BatchService[%s], 获取LUA虚拟机[%s]", ctx.GetPipelineName(), key)

	vm, exist := s.luaVMs[key]
	if exist {
		return vm, nil
	}

	vm = luaengine.New(s.pipeline.EndpointType)
	funcFromProto := vm.NewFunctionFromProto(ctx.GetLuaFunctionProto())
	vm.Push(funcFromProto)
	err := vm.PCall(0, lua.MultRet, nil)
	if err != nil {
		vm.Close()
		return nil, err
	}
	vm.SetGlobal(luaengine.GlobalDataSourceName, lua.LString(s.dataSourceName))
	s.luaVMs[key] = vm
	return vm, nil
}

func (s *BatchService) export(batch int64, ctx *bo.RuleContext) ([]*bo.RowEventRequest, error) {
	if s.shutoff.Load() {
		return nil, errors.Errorf("BatchService[%s] have already shutoff!!", ctx.GetPipelineName())
	}

	offset := s.getOffset(batch)
	fullName := strings.ToLower(ctx.GetRule().Schema + "." + ctx.GetRule().Table)
	statement, _ := s.statements[fullName]
	sql := fmt.Sprintf(statement, offset, s.bulkSize)
	log.Infof("BatchService[%s] 执行导出语句[%s]", ctx.GetPipelineName(), sql)
	resultSet, err := s.connectionPool.Get().Execute(sql)
	if err != nil {
		log.Errorf("BatchService[%s] 数据导出错误[%s]", ctx.GetPipelineName(), err.Error())
		return nil, err
	}
	rowNumber := resultSet.RowNumber()
	requests := make([]*bo.RowEventRequest, 0, rowNumber)
	columnSize := len(ctx.GetTableInfo().Columns)
	for i := 0; i < rowNumber; i++ {
		rowValues := make([]interface{}, 0, columnSize)
		request := bo.BorrowRowEventRequest()
		for j := 0; j < columnSize; j++ {
			var val interface{}
			val, err = resultSet.GetValue(i, j)
			if err != nil {
				log.Errorf("BatchService[%s] 数据导出错误[%s]", ctx.GetPipelineName(), err.Error())
				break
			}
			rowValues = append(rowValues, val)
			request.Context = ctx
			request.Action = canal.InsertAction
			request.Timestamp = 0
			request.PreData = nil
			request.Data = rowValues
		}
		requests = append(requests, request)
	}

	return requests, nil
}

func (s *BatchService) imports(requests []*bo.RowEventRequest, ctx *bo.RuleContext, lvm *lua.LState, counter *atomic.Uint64) error {
	defer func() {
		for _, request := range requests {
			bo.ReleaseRowEventRequest(request)
		}
	}()

	if s.shutoff.Load() {
		return errors.Errorf("BatchService[%s] have already shutoff!!", ctx.GetPipelineName())
	}

	if len(requests) == 0 {
		return nil
	}

	succeeds, err := s.endpoint.Batch(requests, ctx, lvm)
	if err != nil {
		return err
	}

	counter.Add(uint64(succeeds))

	return nil
}

func (s *BatchService) getOffset(batch int64) int64 {
	var offset int64
	if batch > 0 {
		offset = (batch - 1) * s.bulkSize
	}
	return offset
}

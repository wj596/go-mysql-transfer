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

	"go-mysql-transfer/config"
	"go-mysql-transfer/datasource"
	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/domain/vo"
	"go-mysql-transfer/endpoint"
	"go-mysql-transfer/endpoint/luaengine"
	"go-mysql-transfer/util/dateutils"
	"go-mysql-transfer/util/log"
)

var (
	_lockOfBatchService sync.Mutex
	_selectCountSql     = "select count(1) from %s"
)

type BatchService struct {
	runtime        *bo.PipelineRunState
	endpoint       endpoint.IBatchEndpoint
	ruleContexts   map[string]*bo.RuleContext
	connectionPool *datasource.ConnectionPool
	statements     map[string]string
	luaVMs         map[string]*lua.LState
	lockOfLuaVMs   sync.Mutex

	queue          chan []*bo.RowEventRequest
	counters       map[string]int64
	lockOfCounters sync.Mutex
	totalRows      map[string]int64
	wg             sync.WaitGroup
	shutoff        *atomic.Bool
}

func createBatchService(pipelineId uint64, runtime *bo.PipelineRunState) (*BatchService, error) {
	_lockOfBatchService.Lock()
	defer _lockOfBatchService.Unlock()

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

	log.Infof("创建BatchService[%s],SourceInfo: Addr[%s]、User[%s]、Charset[%s]、Flavor[%s]、ServerID[%d]", pipeline.Name, fmt.Sprintf("%s:%d", sourceInfo.Host, sourceInfo.Port), sourceInfo.Username, sourceInfo.Charset, sourceInfo.Flavor, sourceInfo.SlaveID)
	log.Infof("创建BatchService[%s],EndpointInfo: Type[%s]、Addr[%s]、User[%s]", pipeline.Name, constants.GetEndpointTypeName(endpointInfo.GetType()), endpointInfo.GetAddresses(), endpointInfo.GetUsername())

	var rules []*po.TransformRule
	rules, err = _transformRuleService.SelectList(vo.TransformRuleParams{PipelineId: pipelineId})
	if err != nil {
		return nil, err
	}

	var connectionPool *datasource.ConnectionPool
	connectionPool, err = datasource.NewConnectionPool(config.GetIns().GetBatchCoroutines(), sourceInfo)
	if err != nil {
		return nil, err
	}

	contexts := make(map[string]*bo.RuleContext)
	statements := make(map[string]string)
	for _, rule := range rules {
		var tableInfo *schema.Table
		tableInfo, err = connectionPool.Get().GetTable(rule.Schema, rule.Table)
		if err != nil {
			break
		}

		var context *bo.RuleContext
		context, err = bo.CreateRuleContext(pipeline, rule, tableInfo, false)
		if err != nil {
			break
		}
		contexts[context.GetTableFullName()] = context
		statements[context.GetTableFullName()] = buildStatement(context)
	}

	if err != nil {
		connectionPool.Shutdown() //关闭连接池
		return nil, err
	}

	batchService := &BatchService{
		queue:          make(chan []*bo.RowEventRequest, config.GetIns().GetBatchCoroutines()),
		counters:       make(map[string]int64),
		totalRows:      make(map[string]int64),
		shutoff:        atomic.NewBool(false),
		endpoint:       endpoint.NewBatchEndpoint(endpointInfo),
		ruleContexts:   contexts,
		connectionPool: connectionPool,
		statements:     statements,
		runtime:        runtime,
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

func (s *BatchService) start() error {
	defer s.Shutdown()

	if err := s.endpoint.Connect(); err != nil {
		log.Errorf(err.Error())
		return errors.Trace(err)
	}

	s.runtime.SetStatusBatching()

	startTime := dateutils.NowMillisecond()
	for _, ctx := range s.ruleContexts {
		if ctx.GetRule().GetOrderColumn() == "" {
			return errors.New("排序列不能为空")
		}

		tableFullName := ctx.GetTableFullName()
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
		s.totalRows[tableFullName] = totalRow
		s.totalRows[tableFullName] = 0

		var batch int64
		bulkSize := int64(config.GetIns().GetBatchBulkSize())
		if totalRow%bulkSize == 0 {
			batch = totalRow / bulkSize
		} else {
			batch = (totalRow / bulkSize) + 1
		}
		log.Infof("开始导出表[%s]，共[%d]条，[%d]批，每批[%d]条", tableFullName, totalRow, batch, bulkSize)

		var processed atomic.Int64
		for i := 0; i < config.GetIns().GetBatchCoroutines(); i++ {
			var lvm *lua.LState
			if ctx.IsLuaEnable() {
				key := tableFullName + "-" + strconv.Itoa(i)
				lvm, err = s.getOrCreateLuaVM(key, ctx)
				if err != nil {
					return err
				}
			}
			s.wg.Add(1)
			go func(_ctx *bo.RuleContext, _lvm *lua.LState) {
				for {
					processed.Inc()
					var requests []*bo.RowEventRequest
					requests, err = s.export(processed.Load(), _ctx)
					if err != nil {
						log.Error(err.Error())
						s.shutoff.Store(true)
						break
					}

					err = s.imports(requests, _ctx, _lvm)
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
			}(ctx, lvm)
		}
	}

	s.wg.Wait()
	fmt.Println(fmt.Sprintf("共耗时 ：%d（毫秒）", dateutils.NowMillisecond()-startTime))

	for k, v := range s.totalRows {
		vv, ok := s.counters[k]
		if ok {
			fmt.Println(fmt.Sprintf("表： %s，共：%d 条数据，成功导入：%d 条", k, v, vv))
			if v > vv {
				fmt.Println("存在导入错误的数据，具体请至日志查看")
			}
		}
	}

	return nil
}

func (s *BatchService) Shutdown() {
	s.endpoint.Close()          // 关闭客户端
	s.connectionPool.Shutdown() // 关闭数据库连接池
	for _, vm := range s.luaVMs {
		vm.Close() // 关闭LUA虚拟机
	}
	s.runtime.SetStatusBatchEnd()
}

func (s *BatchService) getOrCreateLuaVM(key string, ctx *bo.RuleContext) (*lua.LState, error) {
	s.lockOfLuaVMs.Lock()
	defer s.lockOfLuaVMs.Unlock()

	log.Infof("获取LUA虚拟机[%s]", key)

	vm, exist := s.luaVMs[key]
	if exist {
		return vm, nil
	}

	vm = luaengine.New()
	funcFromProto := vm.NewFunctionFromProto(ctx.GetLuaFunctionProto())
	vm.Push(funcFromProto)
	err := vm.PCall(0, lua.MultRet, nil)
	if err != nil {
		vm.Close()
		return nil, err
	}
	s.luaVMs[key] = vm
	return vm, nil
}

func (s *BatchService) export(batch int64, ctx *bo.RuleContext) ([]*bo.RowEventRequest, error) {
	if s.shutoff.Load() {
		return nil, errors.New("shutoff")
	}

	offset := s.getOffset(batch)
	fullName := strings.ToLower(ctx.GetRule().Schema + "." + ctx.GetRule().Table)
	statement, _ := s.statements[fullName]
	sql := fmt.Sprintf(statement, offset, config.GetIns().GetBatchBulkSize())
	log.Infof("执行导出语句[%s]", sql)
	resultSet, err := s.connectionPool.Get().Execute(sql)
	if err != nil {
		log.Errorf("数据导出错误[%s]", err.Error())
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
				log.Errorf("数据导出错误[%s]", err.Error())
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

func (s *BatchService) imports(requests []*bo.RowEventRequest, ctx *bo.RuleContext, lvm *lua.LState) error {
	defer func() {
		for _, request := range requests {
			bo.ReleaseRowEventRequest(request)
		}
	}()

	if s.shutoff.Load() {
		return errors.New("shutoff")
	}

	succeeds, err := s.endpoint.Batch(requests, ctx, lvm)
	if err != nil {
		return err
	}

	s.lockOfCounters.Lock()
	c, ok := s.counters[ctx.GetTableFullName()]
	if ok {
		c = c + succeeds
		s.counters[ctx.GetTableFullName()] = c
	}
	s.lockOfCounters.Unlock()

	return nil
}

func (s *BatchService) getOffset(batch int64) int64 {
	var offset int64
	if batch > 0 {
		offset = (batch - 1) * int64(config.GetIns().GetBatchBulkSize())
	}
	return offset
}

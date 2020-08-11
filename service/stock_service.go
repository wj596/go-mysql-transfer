package service

import (
	"fmt"
	"log"
	"sync"

	"github.com/juju/errors"

	"go-mysql-transfer/global"
	"go-mysql-transfer/util/dateutil"
	"go-mysql-transfer/util/logutil"
)

// 存量数据
type StockService struct {
	pageSize uint64
	transfer *TransferService

	queueCh       chan []*global.RowRequest
	counter       map[string]int
	lockOfCounter sync.Mutex
	totalRows     map[string]uint64
	wg            sync.WaitGroup
}

func NewStockService(t *TransferService) *StockService {
	return &StockService{
		pageSize:  1024,
		transfer:  t,
		queueCh:   make(chan []*global.RowRequest, 4096),
		counter:   make(map[string]int),
		totalRows: make(map[string]uint64),
	}
}

func (s *StockService) Run() error {
	startTime := dateutil.NowMillisecond()
	for _, rule := range global.RuleInsList() {
		if rule.OrderByColumn == "" {
			return errors.New("empty order_by_column not allowed")
		}
		fullName := fmt.Sprintf("%s.%s", rule.Schema, rule.Table)
		log.Println(fmt.Sprintf("开始导出 %s", fullName))
		res, err := s.transfer.canal.Execute(fmt.Sprintf("select count(1) from %s", fullName))
		if err != nil {
			return err
		}
		totalRow, err := res.GetUint(0, 0)
		s.totalRows[fullName] = totalRow
		log.Println(fmt.Sprintf("%s 共 %d 条数据", fullName, totalRow))

		s.counter[fullName] = 0

		var totalPage uint64
		if totalRow%s.pageSize == 0 {
			totalPage = totalRow / s.pageSize
		} else {
			totalPage = (totalRow / s.pageSize) + 1
		}
		var i uint64
		for i = 1; i <= totalPage; i++ {
			s.wg.Add(1)
			go func(page uint64) {
				s.export(fullName, page, rule)
				s.imports(fullName)
				s.wg.Done()
			}(i)
		}
	}

	s.wg.Wait()

	fmt.Println(fmt.Sprintf("共耗时 ：%d（毫秒）", dateutil.NowMillisecond()-startTime))

	for k, v := range s.totalRows {
		vv, ok := s.counter[k]
		if ok {
			fmt.Println(fmt.Sprintf("表： %s，共：%d 条数据，成功导入：%d 条", k, v, vv))
			if v > uint64(vv) {
				fmt.Println("存在导入错误的数据，具体请至日志查看")
			}
		}
	}

	return nil
}

func (s *StockService) export(fullName string, page uint64, rule *global.Rule) {
	offset := s.offset(page)
	sql := fmt.Sprintf("select * from %s order by %s limit %d,%d", fullName, rule.OrderByColumn, offset, s.pageSize)
	resultSet, err := s.transfer.canal.Execute(sql)
	if err != nil {
		logutil.Errorf(fmt.Sprintf("数据导出错误: %s", err.Error()))
		return
	}
	rowNumber := resultSet.RowNumber()
	requests := make([]*global.RowRequest, 0, rowNumber)
	for i := 0; i < rowNumber; i++ {
		rowValues := make([]interface{}, 0, len(rule.TableInfo.Columns))
		request := new(global.RowRequest)
		for j := 0; j < len(rule.TableInfo.Columns); j++ {
			val, err := resultSet.GetValue(i, j)
			if err != nil {
				logutil.Errorf(fmt.Sprintf("数据导出错误: %s", err.Error()))
				break
			}
			rowValues = append(rowValues, val)
			request.Action = global.InsertAction
			request.RuleKey = global.RuleKey(rule.Schema, rule.Table)
			request.Row = rowValues
		}
		requests = append(requests, request)
	}

	s.queueCh <- requests
}

func (s *StockService) imports(fullName string) {
	requests := <-s.queueCh
	succeeds := s.transfer.endpoint.Stock(requests)
	count := s.incCounter(fullName, succeeds)
	log.Println(fmt.Sprintf("%s 导入数据 %d 条", fullName, count))
}

func (s *StockService) offset(currentPage uint64) uint64 {
	var offset uint64

	if currentPage > 0 {
		offset = (currentPage - 1) * s.pageSize
	}

	return offset
}

func (s *StockService) Close() {
	s.transfer.close()
}

func (s *StockService) incCounter(fullName string, n int) int {
	s.lockOfCounter.Lock()
	defer s.lockOfCounter.Unlock()

	count, ok := s.counter[fullName]
	if ok {
		count = count + n
		s.counter[fullName] = count
	}

	return count
}

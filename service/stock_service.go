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
	"go.uber.org/atomic"
	"log"
	"runtime"
	"sync"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"

	"go-mysql-transfer/global"
	"go-mysql-transfer/util/dateutil"
	"go-mysql-transfer/util/logutil"
)

var _threads = runtime.NumCPU()

// 存量数据
type StockService struct {
	pageSize int64
	transfer *TransferService

	queueCh       chan []*global.RowRequest
	counter       map[string]int64
	lockOfCounter sync.Mutex
	totalRows     map[string]int64
	wg            sync.WaitGroup
	shutoff       *atomic.Bool
}

func NewStockService(t *TransferService) *StockService {
	return &StockService{
		transfer:  t,
		pageSize:  int64(t.config.BulkSize),
		queueCh:   make(chan []*global.RowRequest, _threads),
		counter:   make(map[string]int64),
		totalRows: make(map[string]int64),
		shutoff:   atomic.NewBool(false),
	}
}

func (s *StockService) Run() error {
	startTime := dateutil.NowMillisecond()
	log.Println(fmt.Sprintf("bulk size: %d", s.pageSize))
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
		totalRow, err := res.GetInt(0, 0)
		s.totalRows[fullName] = totalRow
		log.Println(fmt.Sprintf("%s 共 %d 条数据", fullName, totalRow))

		s.counter[fullName] = 0

		var batch int64
		if batch%s.pageSize == 0 {
			batch = totalRow / s.pageSize
		} else {
			batch = (totalRow / s.pageSize) + 1
		}

		var processed atomic.Int64
		for i := 0; i < _threads; i++ {
			s.wg.Add(1)
			go func(_fullName string, _rule *global.Rule) {
				for {
					processed.Inc()
					requests, err := s.export(_fullName, processed.Load(), _rule)
					if err != nil {
						fmt.Println(err.Error())
						s.shutoff.Store(true)
						break
					}

					s.imports(_fullName, requests, processed.Load())
					if processed.Load() > batch {
						break
					}
				}
				s.wg.Done()
			}(fullName, rule)
		}
	}

	s.wg.Wait()

	fmt.Println(fmt.Sprintf("共耗时 ：%d（毫秒）", dateutil.NowMillisecond()-startTime))

	for k, v := range s.totalRows {
		vv, ok := s.counter[k]
		if ok {
			fmt.Println(fmt.Sprintf("表： %s，共：%d 条数据，成功导入：%d 条", k, v, vv))
			if v > vv {
				fmt.Println("存在导入错误的数据，具体请至日志查看")
			}
		}
	}

	return nil
}

func (s *StockService) export(fullName string, batch int64, rule *global.Rule) ([]*global.RowRequest, error) {
	if s.shutoff.Load() {
		log.Println("shutoff at batch :", batch)
		return nil, errors.New("shutoff")
	}

	offset := s.offset(batch)
	sql := fmt.Sprintf("select * from %s order by %s limit %d,%d", fullName, rule.OrderByColumn, offset, s.pageSize)
	resultSet, err := s.transfer.canal.Execute(sql)
	if err != nil {
		logutil.Errorf(fmt.Sprintf("数据导出错误: %s - %s", sql, err.Error()))
		return nil, err
	}
	rowNumber := resultSet.RowNumber()
	requests := make([]*global.RowRequest, 0, rowNumber)
	for i := 0; i < rowNumber; i++ {
		rowValues := make([]interface{}, 0, len(rule.TableInfo.Columns))
		request := new(global.RowRequest)
		for j := 0; j < len(rule.TableInfo.Columns); j++ {
			val, err := resultSet.GetValue(i, j)
			if err != nil {
				logutil.Errorf(fmt.Sprintf("数据导出错误: %s - %s", sql, err.Error()))
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

func (s *StockService) imports(fullName string, requests []*global.RowRequest, processed int64) {
	if s.shutoff.Load() {
		log.Println("shutoff at batch :", processed)
		return
	}

	succeeds := s.transfer.endpoint.Stock(requests)
	count := s.incCounter(fullName, succeeds)
	log.Println(fmt.Sprintf("%s 导入数据 %d 条", fullName, count))
}

func (s *StockService) offset(currentPage int64) int64 {
	var offset int64

	if currentPage > 0 {
		offset = (currentPage - 1) * s.pageSize
	}

	return offset
}

func (s *StockService) Close() {
	s.transfer.close()
}

func (s *StockService) incCounter(fullName string, n int64) int64 {
	s.lockOfCounter.Lock()
	defer s.lockOfCounter.Unlock()

	count, ok := s.counter[fullName]
	if ok {
		count = count + n
		s.counter[fullName] = count
	}

	return count
}

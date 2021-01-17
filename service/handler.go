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
	"go-mysql-transfer/metrics"
	"log"
	"time"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"

	"go-mysql-transfer/global"
	"go-mysql-transfer/model"
	"go-mysql-transfer/util/logs"
)

type handler struct {
	queue chan interface{}
	stop  chan struct{}
}

func newHandler() *handler {
	return &handler{
		queue: make(chan interface{}, 4096),
		stop:  make(chan struct{}, 1),
	}
}

func (s *handler) OnRotate(e *replication.RotateEvent) error {
	s.queue <- model.PosRequest{
		Name:  string(e.NextLogName),
		Pos:   uint32(e.Position),
		Force: true,
	}
	return nil
}

func (s *handler) OnTableChanged(schema, table string) error {
	err := _transferService.updateRule(schema, table)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *handler) OnDDL(nextPos mysql.Position, _ *replication.QueryEvent) error {
	s.queue <- model.PosRequest{
		Name:  nextPos.Name,
		Pos:   nextPos.Pos,
		Force: true,
	}
	return nil
}

func (s *handler) OnXID(nextPos mysql.Position) error {
	s.queue <- model.PosRequest{
		Name:  nextPos.Name,
		Pos:   nextPos.Pos,
		Force: false,
	}
	return nil
}

func (s *handler) OnRow(e *canal.RowsEvent) error {
	ruleKey := global.RuleKey(e.Table.Schema, e.Table.Name)
	if !global.RuleInsExist(ruleKey) {
		return nil
	}

	var requests []*model.RowRequest
	if e.Action != canal.UpdateAction {
		// 定长分配
		requests = make([]*model.RowRequest, 0, len(e.Rows))
	}

	if e.Action == canal.UpdateAction {
		for i := 0; i < len(e.Rows); i++ {
			if (i+1)%2 == 0 {
				v := new(model.RowRequest)
				v.RuleKey = ruleKey
				v.Action = e.Action
				v.Timestamp = e.Header.Timestamp
				if global.Cfg().IsReserveRawData() {
					v.Old = e.Rows[i-1]
				}
				v.Row = e.Rows[i]
				requests = append(requests, v)
			}
		}
	} else {
		for _, row := range e.Rows {
			v := new(model.RowRequest)
			v.RuleKey = ruleKey
			v.Action = e.Action
			v.Timestamp = e.Header.Timestamp
			v.Row = row
			requests = append(requests, v)
		}
	}
	s.queue <- requests

	return nil
}

func (s *handler) OnGTID(gtid mysql.GTIDSet) error {
	return nil
}

func (s *handler) OnPosSynced(pos mysql.Position, set mysql.GTIDSet, force bool) error {
	return nil
}

func (s *handler) String() string {
	return "TransferHandler"
}

func (s *handler) startListener() {
	go func() {
		interval := time.Duration(global.Cfg().FlushBulkInterval)
		bulkSize := global.Cfg().BulkSize
		ticker := time.NewTicker(time.Millisecond * interval)
		defer ticker.Stop()

		lastSavedTime := time.Now()
		requests := make([]*model.RowRequest, 0, bulkSize)
		var current mysql.Position
		from, _ := _transferService.positionDao.Get()
		for {
			needFlush := false
			needSavePos := false
			select {
			case v := <-s.queue:
				switch v := v.(type) {
				case model.PosRequest:
					now := time.Now()
					if v.Force || now.Sub(lastSavedTime) > 3*time.Second {
						lastSavedTime = now
						needFlush = true
						needSavePos = true
						current = mysql.Position{
							Name: v.Name,
							Pos:  v.Pos,
						}
					}
				case []*model.RowRequest:
					requests = append(requests, v...)
					needFlush = int64(len(requests)) >= global.Cfg().BulkSize
				}
			case <-ticker.C:
				needFlush = true
			case <-s.stop:
				return
			}

			if needFlush && len(requests) > 0 && _transferService.endpointEnable.Load() {
				err := _transferService.endpoint.Consume(from, requests)
				if err != nil {
					_transferService.endpointEnable.Store(false)
					metrics.SetDestState(metrics.DestStateFail)
					logs.Error(err.Error())
					go _transferService.stopDump()
				}
				requests = requests[0:0]
			}
			if needSavePos && _transferService.endpointEnable.Load() {
				logs.Infof("save position %s %d", current.Name, current.Pos)
				if err := _transferService.positionDao.Save(current); err != nil {
					logs.Errorf("save sync position %s err %v, close sync", current, err)
					_transferService.Close()
					return
				}
				from = current
			}
		}
	}()
}

func (s *handler) stopListener() {
	log.Println("transfer stop")
	s.stop <- struct{}{}
}

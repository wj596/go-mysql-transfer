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
	"strings"
	"time"

	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"

	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/util/log"
)

const (
	_dumperEventQueueSize = 10240
)

type StreamEventHandler struct {
	serv                  *StreamService
	pipelineId            uint64
	pipelineName          string
	bulkSize              uint64
	flushInterval         uint32
	positionFlushInterval time.Duration
	queue                 chan interface{}
	stopSignal            chan struct{}
}

func newStreamEventHandler(service *StreamService) *StreamEventHandler {
	bulkSize := service.pipeline.StreamBulkSize
	if bulkSize <= 0 {
		bulkSize = uint64(constants.StreamBulkSize)
	}

	flushInterval := service.pipeline.StreamFlushInterval
	if flushInterval <= 0 {
		flushInterval = uint32(constants.StreamFlushInterval)
	}

	positionFlushInterval := constants.PositionFlushInterval
	if flushInterval > 1000 {
		temp := (flushInterval / 1000) * 2
		positionFlushInterval = time.Duration(temp)
	}

	return &StreamEventHandler{
		serv:                  service,
		pipelineId:            service.pipeline.Id,
		pipelineName:          service.pipeline.Name,
		bulkSize:              bulkSize,
		flushInterval:         flushInterval,
		positionFlushInterval: positionFlushInterval,
		queue:                 make(chan interface{}, _dumperEventQueueSize),
		stopSignal:            make(chan struct{}, 1),
	}
}

func (s *StreamEventHandler) OnRow(e *canal.RowsEvent) error {
	key := strings.ToLower(e.Table.Schema + "." + e.Table.Name)
	log.Debugf("OnRow[%s]", key)
	ctx, exist := s.serv.getRuleContext(key)
	if !exist {
		log.Warnf("StreamEventHandler[%s]不存在表[%s]的同步上下文", s.serv.pipeline.Name, key)
		return nil
	}

	length := len(e.Rows)
	var requests []*bo.RowEventRequest
	if e.Action != canal.UpdateAction {
		requests = make([]*bo.RowEventRequest, 0, length) // 定长分配
	}

	if canal.UpdateAction == e.Action {
		for i := 0; i < length; i++ {
			if (i+1)%2 == 0 {
				v := bo.BorrowRowEventRequest()
				v.Context = ctx
				v.Action = e.Action
				v.Timestamp = e.Header.Timestamp
				v.PreData = nil
				if ctx.IsReservePreData() { //是否接收覆盖之前的数据
					v.PreData = e.Rows[i-1]
				}
				v.Data = e.Rows[i]
				requests = append(requests, v)
			}
		}
	} else {
		for _, row := range e.Rows {
			v := bo.BorrowRowEventRequest()
			v.Context = ctx
			v.Action = e.Action
			v.Timestamp = e.Header.Timestamp
			v.Data = row
			requests = append(requests, v)
		}
	}

	s.serv.runtime.AddCount(e.Action, len(requests))
	s.queue <- requests

	return nil
}

func (s *StreamEventHandler) start() {
	go func() {
		log.Infof("StreamEventHandler[%s]启动事件监听", s.pipelineName)
		ticker := time.NewTicker(time.Millisecond * time.Duration(s.flushInterval))
		defer ticker.Stop()

		lastPositionSaveTime := time.Now()
		sends := make([]*bo.RowEventRequest, 0, s.bulkSize)
		var current mysql.Position
		for {
			needFlush := false
			needSavePosition := false
			select {
			case v := <-s.queue:
				switch vv := v.(type) {
				case bo.StreamStopEventRequest:
					needFlush = true
					needSavePosition = true
				case bo.PositionEventRequest:
					now := time.Now()
					current = mysql.Position{
						Name: vv.Name,
						Pos:  vv.Position,
					}
					if vv.Force || now.Sub(lastPositionSaveTime) > s.positionFlushInterval*time.Second {
						lastPositionSaveTime = now
						needFlush = true
						needSavePosition = true
					}
				case []*bo.RowEventRequest:
					sends = append(sends, vv...)
					needFlush = uint64(len(sends)) >= s.bulkSize
				}
			case <-ticker.C:
				needFlush = true
			case <-s.stopSignal:
				log.Infof("StreamEventHandler[%s]停止事件监听", s.pipelineName)
				return
			}
			//刷新数据
			if needFlush && len(sends) > 0 && s.serv.isEndpointEnable() {
				log.Infof("StreamEventHandler[%s]刷新[%d]条数据", s.pipelineName, len(sends))
				err := s.serv.endpoint.Stream(sends)
				if err != nil {
					log.Errorf("StreamEventHandler[%s]刷新数据失败[%s]", s.pipelineName, err.Error())
					if err == constants.LuaScriptError {
						streamServicePanic(s.serv, "Lua脚本执行失败")
					} else {
						s.serv.endpointFault(fmt.Sprintf("端点故障[%s]", err.Error()))
					}
				}
				sends = sends[0:0]
			}

			//刷新Position
			if needSavePosition && s.serv.isEndpointEnable() {
				log.Infof("StreamEventHandler[%s]刷新Position[%s %d]", s.pipelineName, current.Name, current.Pos)
				if err := _positionService.update(s.pipelineId, current); err != nil {
					log.Errorf("StreamEventHandler[%s]刷新Position失败[%s]", s.pipelineName, err.Error())
					streamServicePanic(s.serv, fmt.Sprintf("刷新Position失败[%s]", err.Error()))
				}
			}
		}
	}()
}

func (s *StreamEventHandler) stop() {
	s.stopSignal <- struct{}{}
}

func (s *StreamEventHandler) flushQueue() {
	s.queue <- bo.StreamStopEventRequest{}
}

func (s *StreamEventHandler) String() string {
	return "StreamEventHandler[" + s.pipelineName + "]"
}

func (s *StreamEventHandler) OnGTID(gtid mysql.GTIDSet) error {
	return nil
}

func (s *StreamEventHandler) OnPosSynced(pos mysql.Position, set mysql.GTIDSet, force bool) error {
	return nil
}

func (s *StreamEventHandler) OnXID(nextPos mysql.Position) error {
	s.queue <- bo.PositionEventRequest{
		Name:     nextPos.Name,
		Position: nextPos.Pos,
		Force:    false,
	}
	return nil
}

func (s *StreamEventHandler) OnRotate(e *replication.RotateEvent) error {
	s.queue <- bo.PositionEventRequest{
		Name:     string(e.NextLogName),
		Position: uint32(e.Position),
		Force:    true,
	}
	return nil
}

func (s *StreamEventHandler) OnDDL(nextPos mysql.Position, _ *replication.QueryEvent) error {
	s.queue <- bo.PositionEventRequest{
		Name:     nextPos.Name,
		Position: nextPos.Pos,
		Force:    true,
	}
	return nil
}

func (s *StreamEventHandler) OnTableChanged(schema, table string) error {
	log.Infof("StreamService[%s] OnTableChanged", s.serv.pipeline.Name)
	// onTableStructureChanged
	return nil
}

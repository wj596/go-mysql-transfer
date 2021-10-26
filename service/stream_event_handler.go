package service

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"

	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/util/log"
)

const _dumperEventQueueSize = 10240

type StreamEventHandler struct {
	serv       *StreamService
	pipelineId string

	queue      chan interface{}
	stopSignal chan struct{}
}

func newStreamEventHandler(service *StreamService) *StreamEventHandler {
	return &StreamEventHandler{
		serv:       service,
		pipelineId: strconv.FormatUint(service.getPipelineId(), 10),

		queue:      make(chan interface{}, _dumperEventQueueSize),
		stopSignal: make(chan struct{}, 1),
	}
}

func (s *StreamEventHandler) OnRow(e *canal.RowsEvent) error {
	key := strings.ToLower(e.Table.Schema + "." + e.Table.Name)
	ctx, exist := s.serv.getRuleContext(key)
	if !exist {
		log.Warnf("[%s] 不存在表[%s]的同步上下文", s.serv.getPipelineName(), key)
		return nil
	}

	length := len(e.Rows)
	s.serv.addHandleCount(e.Action, length)

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
	s.queue <- requests
	return nil
}

func (s *StreamEventHandler) start() {
	go func() {
		log.Infof("[%s] 启动事件监听", s.serv.getPipelineName())
		bulkSize := s.serv.pipeline.FlushBulkSize
		flushInterval := time.Duration(s.serv.pipeline.FlushBulkInterval)
		ticker := time.NewTicker(time.Millisecond * flushInterval)
		defer ticker.Stop()

		lastPositionSaveTime := time.Now()
		sends := make([]*bo.RowEventRequest, 0, bulkSize)
		var current mysql.Position
		for {
			needFlush := false
			needSavePosition := false
			select {
			case v := <-s.queue:
				switch v := v.(type) {
				case bo.PositionEventRequest:
					now := time.Now()
					if v.Force || now.Sub(lastPositionSaveTime) > 3*time.Second {
						lastPositionSaveTime = now
						needFlush = true
						needSavePosition = true
						current = mysql.Position{
							Name: v.Name,
							Pos:  v.Position,
						}
					}
				case []*bo.RowEventRequest:
					sends = append(sends, v...)
					needFlush = uint64(len(sends)) >= bulkSize
				}
			case <-ticker.C:
				needFlush = true
			case <-s.stopSignal:
				log.Infof("[%s] 停止事件监听", s.serv.getPipelineName())
				return
			}

			//刷新数据
			if needFlush && len(sends) > 0 && s.serv.isEndpointEnable() {
				log.Infof("[%s] 刷新[%d]条数据", s.serv.getPipelineName(), len(sends))
				err := s.serv.endpoint.Stream(sends)
				if err != nil {
					log.Errorf("[%s] 刷新数据失败[%s]", s.serv.getPipelineName(), err.Error())
					if err == constants.LuaScriptError {
						closeStreamService(s.serv, "Lua脚本执行失败")
					} else {
						s.serv.endpointFault(fmt.Sprintf("端点故障[%s]", err.Error()))
					}
				}
				sends = sends[0:0]
			}

			//刷新Position
			if needSavePosition && s.serv.isEndpointEnable() {
				s.serv.runtime.SetPosition(current)
				_streamStateService.SaveStreamCounts(s.serv.getPipelineId(), s.serv.runtime)
				log.Infof("[%s] 刷新Position[%s %d]", s.serv.getPipelineName(), current.Name, current.Pos)
				if err := _streamStateService.SavePosition(s.serv.getPipelineId(), current); err != nil {
					log.Errorf("[%s] 刷新Position失败[%s]", s.serv.getPipelineName(), err.Error())
					closeStreamService(s.serv, fmt.Sprintf("刷新Position失败[%s]", err.Error()))
				}
			}
		}
	}()
}

func (s *StreamEventHandler) stop() {
	s.stopSignal <- struct{}{}
}

func (s *StreamEventHandler) String() string {
	return "StreamService[" + s.serv.getPipelineName() + "]"
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
	log.Infof("StreamService[%s] OnTableChanged", s.serv.getPipelineName())
	// onTableStructureChanged
	return nil
}

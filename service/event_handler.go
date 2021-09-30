package service

import (
	"strconv"
	"strings"
	"time"

	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"

	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/util/log"
)

const _eventQueueSize = 10240

type eventHandler struct {
	dump   *dumper
	queue  chan interface{}
	signal chan struct{}
}

func newEventHandler(dump *dumper) *eventHandler {
	return &eventHandler{
		dump:   dump,
		queue:  make(chan interface{}, _eventQueueSize),
		signal: make(chan struct{}, 1),
	}
}

func (s *eventHandler) OnGTID(gtid mysql.GTIDSet) error {
	log.Infof("Pipeline[%s] OnGTID", s.getPipelineName())
	return nil
}

func (s *eventHandler) OnPosSynced(pos mysql.Position, set mysql.GTIDSet, force bool) error {
	log.Infof("Pipeline[%s] OnPosSynced", s.getPipelineName())
	return nil
}

func (s *eventHandler) OnRotate(e *replication.RotateEvent) error {
	log.Infof("Pipeline[%s] OnRotate", s.getPipelineName())
	s.queue <- bo.PositionEventRequest{
		Name:     string(e.NextLogName),
		Position: uint32(e.Position),
		Force:    true,
	}
	return nil
}

func (s *eventHandler) OnDDL(nextPos mysql.Position, _ *replication.QueryEvent) error {
	log.Infof("Pipeline[%s] OnDDL", s.getPipelineName())
	s.queue <- bo.PositionEventRequest{
		Name:     nextPos.Name,
		Position: nextPos.Pos,
		Force:    true,
	}
	return nil
}

func (s *eventHandler) OnXID(nextPos mysql.Position) error {
	log.Infof("Pipeline[%s] OnXID", s.getPipelineName())
	s.queue <- bo.PositionEventRequest{
		Name:     nextPos.Name,
		Position: nextPos.Pos,
		Force:    false,
	}
	return nil
}

func (s *eventHandler) OnTableChanged(schema, table string) error {
	log.Infof("Pipeline[%s] OnTableChanged", s.getPipelineName())
	// onTableStructureChanged
	return nil
}

func (s *eventHandler) OnRow(e *canal.RowsEvent) error {
	log.Infof("Pipeline[%s]实例， OnRow", s.getPipelineName())
	ruleKey := strings.ToLower(strconv.FormatUint(s.dump.pipeline.Id, 10) + "." + e.Table.Schema + "." + e.Table.Name)
	rule, exist := bo.RuntimeRules[ruleKey]
	if !exist {
		log.Warnf("Pipeline[%s],不存在表[%s]的同步规则", s.getPipelineName(), ruleKey)
		return nil
	}

	var requests []*bo.RowEventRequest
	if e.Action != canal.UpdateAction {
		requests = make([]*bo.RowEventRequest, 0, len(e.Rows)) // 定长分配
	}

	switch e.Action {
	case canal.UpdateAction:
		for i := 0; i < len(e.Rows); i++ {
			if (i+1)%2 == 0 {
				v := bo.BorrowRowEventRequest()
				v.RuleKey = ruleKey
				v.Action = e.Action
				v.Timestamp = e.Header.Timestamp
				if rule.IsReserveCoveredData() {
					v.Covered = e.Rows[i-1]
				}
				v.Row = e.Rows[i]
				requests = append(requests, v)
			}
		}
	default:
		for _, row := range e.Rows {
			v := bo.BorrowRowEventRequest()
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

func (s *eventHandler) start() {
	go func() {
		log.Infof("Pipeline[%s] Start EventHandler", s.getPipelineName())
		bulkSize := s.dump.pipeline.FlushBulkSize
		interval := time.Duration(s.dump.pipeline.FlushBulkInterval)
		ticker := time.NewTicker(time.Millisecond * interval)
		defer ticker.Stop()

		lastSavedTime := time.Now()
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
					if v.Force || now.Sub(lastSavedTime) > 3*time.Second {
						lastSavedTime = now
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
			case <-s.signal:
				return
			}

			if needFlush && len(sends) > 0 && s.dump.isEndpointEnable() {
				err := s.dump.endpoint.Consume(sends)
				if err != nil {
					log.Errorf("Pipeline[%s] Endpoint Error[%s]", s.getPipelineName(), err.Error())
					s.dump.setEndpointEnable(false)
					go s.dump.stop()
					// TODO metrics
					//metrics.SetDestState(metrics.DestStateFail)
				}
				sends = sends[0:0]
			}

			if needSavePosition && s.dump.isEndpointEnable() {
				log.Infof("Pipeline[%s],save position[%s %d]", s.getPipelineName(), current.Name, current.Pos)
				//if err := s.infoDao.UpdatePosition(s.info.Id, current); err != nil {
				//	log.Errorf("Pipeline[%s],save position error[%s], close pipeline", s.info.Name, err.Error())
				//	s.Close() // 关闭Pipeline
				//	return
				//}
			}
		}
	}()
}

func (s *eventHandler) stop() {
	log.Infof("Pipeline[%s] Stop EventHandler", s.getPipelineName())
	s.signal <- struct{}{}
}

func (s *eventHandler) String() string {
	return "Pipeline[" + s.getPipelineName() + "]"
}

func (s *eventHandler) getPipelineName() string {
	return s.dump.pipeline.Name
}

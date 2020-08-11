package service

import (
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"go-mysql-transfer/global"
	"go-mysql-transfer/util/logutil"
	"time"
)

type handler struct {
	transfer *TransferService

	requestQueue chan interface{}
}

func (h *handler) OnRotate(e *replication.RotateEvent) error {
	h.requestQueue <- global.PosRequest{
		Name:  string(e.NextLogName),
		Pos:   uint32(e.Position),
		Force: true,
	}

	return h.transfer.CtxErr()
}

func (h *handler) OnTableChanged(schema, table string) error {
	err := h.transfer.updateRule(schema, table)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (h *handler) OnDDL(nextPos mysql.Position, _ *replication.QueryEvent) error {
	h.requestQueue <- global.PosRequest{
		Name:  nextPos.Name,
		Pos:   nextPos.Pos,
		Force: true,
	}

	return h.transfer.CtxErr()
}

func (h *handler) OnXID(nextPos mysql.Position) error {
	h.requestQueue <- global.PosRequest{
		Name:  nextPos.Name,
		Pos:   nextPos.Pos,
		Force: false,
	}

	return h.transfer.CtxErr()
}

func (h *handler) OnRow(e *canal.RowsEvent) error {
	ruleKey := global.RuleKey(e.Table.Schema, e.Table.Name)
	if !global.RuleInsExist(ruleKey) {
		return nil
	}

	var requests []*global.RowRequest
	for _, row := range e.Rows {
		requests = append(requests, &global.RowRequest{
			RuleKey: ruleKey,
			Action:  h.actionType(e.Action),
			Row:     row,
		})
	}
	h.requestQueue <- requests

	return h.transfer.CtxErr()
}

func (h *handler) OnGTID(gtid mysql.GTIDSet) error {
	return nil
}

func (h *handler) OnPosSynced(pos mysql.Position, set mysql.GTIDSet, force bool) error {
	return nil
}

func (h *handler) String() string {
	return "TransferHandler"
}

func (h *handler) startRequestQueueListener() {
	go func() {
		h.transfer.listenerStarted.Store(true)
		interval := time.Duration(h.transfer.config.FlushBulkInterval)
		bulkSize := h.transfer.config.BulkSize

		ticker := time.NewTicker(time.Millisecond * interval)
		defer ticker.Stop()
		defer h.transfer.wg.Done()

		lastSavedTime := time.Now()
		requests := make([]*global.RowRequest, 0, bulkSize)
		var current mysql.Position

		for {
			needFlush := false
			needSavePos := false
			select {
			case v := <-h.requestQueue:
				switch v := v.(type) {
				case global.PosRequest:
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
				case []*global.RowRequest:
					requests = append(requests, v...)
					needFlush = len(requests) >= h.transfer.config.BulkSize
				}
			case <-ticker.C:
				needFlush = true
			case <-h.transfer.CtxDone():
				return
			}

			if needFlush {
				if len(requests) > 0 {
					h.transfer.endpoint.Consume(requests)
					requests = requests[0:0]
				}
			}
			if needSavePos {
				logutil.Infof("save position %s %d", current.Name, current.Pos)
				if err := h.transfer.positionStorage.Save(current); err != nil {
					logutil.Errorf("save sync position %s err %v, close sync", current, err)
					h.transfer.cancelFunc()
					return
				}
			}
		}
	}()
}

func (h *handler) actionType(canalAction string) int {
	switch canalAction {
	case canal.InsertAction:
		return global.InsertAction
	case canal.DeleteAction:
		return global.DeleteAction
	case canal.UpdateAction:
		return global.UpdateAction
	}
	return 0
}

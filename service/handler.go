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
	"time"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"

	"go-mysql-transfer/global"
	"go-mysql-transfer/util/logutil"
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

	return h.transfer.ctx.Err()
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

	return h.transfer.ctx.Err()
}

func (h *handler) OnXID(nextPos mysql.Position) error {
	h.requestQueue <- global.PosRequest{
		Name:  nextPos.Name,
		Pos:   nextPos.Pos,
		Force: false,
	}

	return h.transfer.ctx.Err()
}

func (h *handler) OnRow(e *canal.RowsEvent) error {
	ruleKey := global.RuleKey(e.Table.Schema, e.Table.Name)
	if !global.RuleInsExist(ruleKey) {
		return nil
	}

	var requests []*global.RowRequest
	if e.Action == canal.UpdateAction {
		for i := 0; i < len(e.Rows); i++ {
			if (i+1)%2 == 0 {
				rr := global.RowRequestPool.Get().(*global.RowRequest)
				rr.RuleKey = ruleKey
				rr.Action = e.Action
				rr.Row = e.Rows[i]
				requests = append(requests, rr)
			}
		}
	} else {
		for _, row := range e.Rows {
			rr := global.RowRequestPool.Get().(*global.RowRequest)
			rr.RuleKey = ruleKey
			rr.Action = e.Action
			rr.Row = row
			requests = append(requests, rr)
		}
	}
	h.requestQueue <- requests

	return h.transfer.ctx.Err()
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
			case <-h.transfer.ctx.Done():
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

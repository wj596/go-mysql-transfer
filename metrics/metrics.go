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
package metrics

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/siddontang/go-mysql/canal"
	"go.uber.org/atomic"

	"go-mysql-transfer/global"
)

const (
	DestStateOK   = 1
	DestStateFail = 0

	LeaderState   = 1
	FollowerState = 0
)

var (
	leaderState  atomic.Bool
	destState    atomic.Bool
	delay        atomic.Uint32
	insertRecord map[string]*atomic.Uint64
	updateRecord map[string]*atomic.Uint64
	deleteRecord map[string]*atomic.Uint64
)

var (
	leaderStateGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "transfer_leader_state",
			Help: "The cluster leader state: 0=false, 1=true",
		},
	)

	destStateGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "transfer_destination_state",
			Help: "The destination running state: 0=stopped, 1=ok",
		},
	)

	delayGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "transfer_delay",
			Help: "The transfer slave lag",
		},
	)

	insertCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "transfer_inserted_num",
			Help: "The number of data inserted to destination",
		}, []string{"table"},
	)

	updateCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "transfer_updated_num",
			Help: "The number of data updated to destination",
		}, []string{"table"},
	)

	deleteCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "transfer_deleted_num",
			Help: "The number of data deleted from destination",
		}, []string{"table"},
	)
)

func Initialize() error {
	if global.Cfg().EnableExporter {
		go func() {
			http.Handle("/", promhttp.Handler())
			http.ListenAndServe(fmt.Sprintf(":%d", global.Cfg().ExporterPort), nil)
		}()
	}
	if global.Cfg().EnableWebAdmin {
		insertRecord = make(map[string]*atomic.Uint64)
		updateRecord = make(map[string]*atomic.Uint64)
		deleteRecord = make(map[string]*atomic.Uint64)
		for _, k := range global.RuleKeyList() {
			insertRecord[k] = &atomic.Uint64{}
			updateRecord[k] = &atomic.Uint64{}
			deleteRecord[k] = &atomic.Uint64{}
		}
	}
	return nil
}

func SetLeaderState(state int) {
	if global.Cfg().EnableExporter {
		leaderStateGauge.Set(float64(state))
	}
	if global.Cfg().EnableWebAdmin {
		leaderState.Store(LeaderState == state)
	}
}

func SetDestState(state int) {
	if global.Cfg().EnableExporter {
		destStateGauge.Set(float64(state))
	}
	if global.Cfg().EnableWebAdmin {
		destState.Store(DestStateOK == state)
	}
}

func DestState() bool {
	return destState.Load()
}

func SetTransferDelay(d uint32) {
	if global.Cfg().EnableExporter {
		delayGauge.Set(float64(d))
	}
	if global.Cfg().EnableWebAdmin {
		delay.Store(d)
	}
}

func UpdateActionNum(action, lab string) {
	if global.Cfg().EnableExporter {
		switch action {
		case canal.InsertAction:
			insertCounter.WithLabelValues(lab).Inc()
		case canal.UpdateAction:
			updateCounter.WithLabelValues(lab).Inc()
		case canal.DeleteAction:
			deleteCounter.WithLabelValues(lab).Inc()
		}
	}
	if global.Cfg().EnableWebAdmin {
		switch action {
		case canal.InsertAction:
			if v, ok := insertRecord[lab]; ok {
				v.Inc()
			}
		case canal.UpdateAction:
			if v, ok := updateRecord[lab]; ok {
				v.Inc()
			}
		case canal.DeleteAction:
			if v, ok := deleteRecord[lab]; ok {
				v.Inc()
			}
		}
	}
}

func InsertAmount() uint64 {
	var amount uint64
	for _, v := range insertRecord {
		amount += v.Load()
	}
	return amount
}

func UpdateAmount() uint64 {
	var amount uint64
	for _, v := range updateRecord {
		amount += v.Load()
	}
	return amount
}

func DeleteAmount() uint64 {
	var amount uint64
	for _, v := range deleteRecord {
		amount += v.Load()
	}
	return amount
}

func LabInsertAmount(lab string) uint64 {
	var nn uint64
	n, ok := insertRecord[lab]
	if ok {
		nn = n.Load()
	}
	return nn
}

func LabUpdateRecord(lab string) uint64 {
	var nn uint64
	n, ok := updateRecord[lab]
	if ok {
		nn = n.Load()
	}
	return nn
}

func LabDeleteRecord(lab string) uint64 {
	var nn uint64
	n, ok := deleteRecord[lab]
	if ok {
		nn = n.Load()
	}
	return nn
}

func LeaderFlag() bool {
	return leaderState.Load()
}

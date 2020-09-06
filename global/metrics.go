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
package global

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	MetricsStateOK = 1
	MetricsStateNO = 0
)

var (
	leaderState = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "transfer_leader_state",
			Help: "The cluster leader state: 0=false, 1=true",
		},
	)

	destinationState = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "transfer_destination_state",
			Help: "The destination running state: 0=stopped, 1=ok",
		},
	)

	transferDelay = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "transfer_delay",
			Help: "The transfer slave lag",
		},
	)

	insertNum = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "transfer_inserted_num",
			Help: "The number of data inserted to destination",
		}, []string{"table"},
	)

	updateNum = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "transfer_updated_num",
			Help: "The number of data updated to destination",
		}, []string{"table"},
	)

	deleteNum = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "transfer_deleted_num",
			Help: "The number of data deleted from destination",
		}, []string{"table"},
	)
)

func SetLeaderState(state int) {
	if _config.EnableExporter {
		leaderState.Set(float64(state))
	}
}

func SetDestinationState(state int) {
	destinationState.Set(float64(state))
}

func SetTransferDelay(delay uint32) {
	transferDelay.Set(float64(delay))
}

func IncInsertNum(lab string) {
	insertNum.WithLabelValues(lab).Inc()
}

func IncUpdateNum(lab string) {
	updateNum.WithLabelValues(lab).Inc()
}

func IncDeleteNum(lab string) {
	deleteNum.WithLabelValues(lab).Inc()
}

func StartMonitor() {
	if _config.EnableExporter {
		go func() {
			http.Handle("/", promhttp.Handler())
			http.ListenAndServe(fmt.Sprintf(":%d", _config.ExporterPort), nil)
		}()
	}
}

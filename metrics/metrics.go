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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/atomic"
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
	return nil
}

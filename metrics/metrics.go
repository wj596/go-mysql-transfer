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
	"github.com/rcrowley/go-metrics"
	"go.uber.org/atomic"
)

const (
	DestStateOK   = 1
	DestStateFail = 0

	LeaderState   = 1
	FollowerState = 0
)

var (
	leaderState atomic.Bool
	_gauges     = make(map[string]metrics.Gauge)
	_counters   = make(map[string]metrics.Counter)
	_meters     = make(map[string]metrics.Counter)
)

func Initialize() error {
	return nil
}

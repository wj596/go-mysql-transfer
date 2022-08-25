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

package bo

import "go-mysql-transfer/util/dateutils"

type ClusterNode struct {
	Addr           string `json:"addr"`           //节点地址
	IsLeader       bool   `json:"isLeader"`       //是否主节点
	LastActiveTime int64  `json:"lastActiveTime"` //最后活跃时间
	Deadline       bool   `json:"deadline"`       //是否离线
}

type SyncEvent struct {
	Id        uint64
	Type      int32
	Version   int32
	Timestamp int64
}

type AlarmEvent struct {
	Id          uint64
	DataVersion int32
	Timestamp   int64
}

type DispatchEvent struct {
	Timestamp int64
	Reason    string
}

func NewDispatchEvent(reason string) *DispatchEvent {
	return &DispatchEvent{
		Timestamp: dateutils.NowMillisecond(),
		Reason:    reason,
	}
}

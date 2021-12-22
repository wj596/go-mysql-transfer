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

package vo

import (
	"go-mysql-transfer/domain/po"
)

// PipelineInfoVO '通道信息'值对象
type PipelineInfoVO struct {
	Id                    uint64    `json:"id,string"`
	Name                  string    `json:"name"`
	SourceId              uint64    `json:"sourceId,string"`
	EndpointId            uint64    `json:"endpointId,string"`
	EndpointType          uint32    `json:"endpointType"`
	SourceName            string    `json:"sourceName"`
	EndpointName          string    `json:"endpointName"`
	CreateTime            string    `json:"createTime"`
	UpdateTime            string    `json:"updateTime"`
	Status                uint32    `json:"status,string"`
	StreamBulkSize        uint64    `json:"streamBulkSize"`
	StreamFlushInterval   uint32    `json:"streamFlushInterval"`
	BatchCoroutines       uint32    `json:"batchCoroutines"`
	BatchBulkSize         uint32    `json:"batchBulkSize"`
	AlarmMailList         string    `json:"alarmMailList"`
	AlarmWebhook          string    `json:"alarmWebhook"`
	AlarmWebhookSecretKey string    `json:"alarmWebhookSecretKey"`
	AlarmItemList         string    `json:"alarmItemList"`
	Rules                 []*RuleVO `json:"rules"`
}

func (s *PipelineInfoVO) ToPO() *po.PipelineInfo {
	p := &po.PipelineInfo{
		Id:                    s.Id,
		Name:                  s.Name,
		SourceId:              s.SourceId,
		EndpointId:            s.EndpointId,
		EndpointType:          s.EndpointType,
		CreateTime:            s.CreateTime,
		Status:                s.Status,
		StreamBulkSize:        s.StreamBulkSize,
		StreamFlushInterval:   s.StreamFlushInterval,
		BatchCoroutines:       s.BatchCoroutines,
		BatchBulkSize:         s.BatchBulkSize,
		AlarmMailList:         s.AlarmMailList,
		AlarmWebhook:          s.AlarmWebhook,
		AlarmWebhookSecretKey: s.AlarmWebhookSecretKey,
		AlarmItemList:         s.AlarmItemList,
	}
	return p
}

func (s *PipelineInfoVO) FromPO(p *po.PipelineInfo) {
	s.Id = p.Id
	s.Name = p.Name
	s.SourceId = p.SourceId
	s.EndpointId = p.EndpointId
	s.EndpointType = p.EndpointType
	s.CreateTime = p.CreateTime
	s.UpdateTime = p.UpdateTime
	s.Status = p.Status
	s.StreamBulkSize = p.StreamBulkSize
	s.StreamFlushInterval = p.StreamFlushInterval
	s.BatchCoroutines = p.BatchCoroutines
	s.BatchBulkSize = p.BatchBulkSize
	s.AlarmMailList = p.AlarmMailList
	s.AlarmWebhook = p.AlarmWebhook
	s.AlarmWebhookSecretKey = p.AlarmWebhookSecretKey
	s.AlarmItemList = p.AlarmItemList
}

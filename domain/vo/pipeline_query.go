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
	"go-mysql-transfer/util/pageutils"
)

type PipelineInfoParams struct {
	page         *pageutils.PageRequest
	Name         string
	SourceId     uint64
	EndpointId   uint64
	EndpointType uint32
	Enable       bool
}

type PipelineInfoResp struct {
	Total int               `json:"total"` // 总条数
	Items []*PipelineInfoVO `json:"items"` // 查询结果
}

func NewPipelineInfoParams() *PipelineInfoParams {
	return &PipelineInfoParams{
		page: new(pageutils.PageRequest),
	}
}

func (s *PipelineInfoParams) SetName(name string) *PipelineInfoParams {
	s.Name = name
	return s
}

func (s *PipelineInfoParams) SetSourceID(sourceId uint64) *PipelineInfoParams {
	s.SourceId = sourceId
	return s
}

func (s *PipelineInfoParams) SetEndpointID(endpointId uint64) *PipelineInfoParams {
	s.EndpointId = endpointId
	return s
}

func (s *PipelineInfoParams) Page() *pageutils.PageRequest {
	return s.page
}

func NewPipelineInfoResp() *PipelineInfoResp {
	return &PipelineInfoResp{}
}

func (s *PipelineInfoResp) SetTotal(total int) *PipelineInfoResp {
	s.Total = total
	return s
}

func (s *PipelineInfoResp) SetItems(items []*PipelineInfoVO) *PipelineInfoResp {
	s.Items = items
	return s
}

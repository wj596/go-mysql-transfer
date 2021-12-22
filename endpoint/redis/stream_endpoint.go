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

package redis

import (
	"github.com/go-redis/redis"

	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/util/log"
)

type StreamEndpoint struct {
	endpoint *Endpoint
	pipeline redis.Pipeliner
}

func NewStreamEndpoint(info *po.EndpointInfo) *StreamEndpoint {
	return &StreamEndpoint{
		endpoint: &Endpoint{
			info: info,
		},
	}
}

func (s *StreamEndpoint) Stream(requests []*bo.RowEventRequest) error {
	var err error
	for _, request := range requests {
		ctx := request.Context
		if ctx.GetTableColumnCount() != len(request.Data) {
			log.Warnf("管道[%s]，表[%s]的结构发生变更，忽略此条数据", ctx.GetPipelineName(), ctx.GetTableFullName())
			continue
		}

		if ctx.IsLuaEnable() {
			err = s.endpoint.parseByLua(request, ctx, s.pipeline, nil)
			if err != nil {
				return err
			}
		} else {
			err = s.endpoint.parseByRegular(request, ctx, s.pipeline)
			if err != nil {
				return err
			}
		}
	}

	_, err = s.pipeline.Exec()
	return err
}

func (s *StreamEndpoint) Connect() error {
	err := s.endpoint.Connect()
	if err == nil {
		s.pipeline = s.endpoint.createPipeline()
	}
	return err
}

func (s *StreamEndpoint) Ping() error {
	return s.endpoint.Ping()
}

func (s *StreamEndpoint) Close() {
	s.pipeline.Close()
	s.endpoint.Close()
}

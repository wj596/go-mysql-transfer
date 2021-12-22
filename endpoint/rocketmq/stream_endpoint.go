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

package rocketmq

import (
	"context"
	"sync"

	"github.com/apache/rocketmq-client-go/v2/primitive"

	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/util/log"
)

type StreamEndpoint struct {
	endpoint *Endpoint
}

func NewStreamEndpoint(info *po.EndpointInfo) *StreamEndpoint {
	return &StreamEndpoint{
		endpoint: &Endpoint{
			info: info,
		},
	}
}

func (s *StreamEndpoint) Stream(requests []*bo.RowEventRequest) error {
	var messages []*primitive.Message
	for _, request := range requests {
		ctx := request.Context
		if ctx.GetTableColumnCount() != len(request.Data) {
			log.Warnf("管道[%s]，表[%s]的结构发生变更，忽略此条数据", ctx.GetPipelineName(), ctx.GetTableFullName())
			continue
		}

		if ctx.IsLuaEnable() {
			err := s.endpoint.parseByLua(request, ctx, messages, nil)
			if err != nil {
				return err
			}
		} else {
			err := s.endpoint.parseByRegular(request, ctx, messages)
			if err != nil {
				return err
			}
		}
	}

	if len(messages) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	wg.Add(1)
	var callbackErr error
	err := s.endpoint.client.SendAsync(context.Background(),
		func(ctx context.Context, result *primitive.SendResult, e error) {
			if e != nil {
				callbackErr = e
			}
			wg.Done()
		}, messages...)

	if err != nil {
		return err
	}
	wg.Wait()

	return callbackErr
}

func (s *StreamEndpoint) Connect() error {
	return s.endpoint.Connect()
}

func (s *StreamEndpoint) Ping() error {
	return s.endpoint.Ping()
}

func (s *StreamEndpoint) Close() {
	s.endpoint.Close()
}

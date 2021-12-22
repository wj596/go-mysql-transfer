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

package elasticsearch6

import (
	"context"

	"github.com/juju/errors"
	"github.com/yuin/gopher-lua"

	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/util/log"
)

type BatchEndpoint struct {
	endpoint *Endpoint
}

func NewBatchEndpoint(info *po.EndpointInfo) *BatchEndpoint {
	return &BatchEndpoint{
		endpoint: &Endpoint{
			info: info,
		},
	}
}

func (s *BatchEndpoint) Batch(requests []*bo.RowEventRequest, ctx *bo.RuleContext, lvm *lua.LState) (int64, error) {
	if len(requests) == 0 {
		return 0, nil
	}

	bulkService := s.endpoint.client.Bulk()
	if ctx.IsLuaEnable() {
		for _, request := range requests {
			err := s.endpoint.parseByLua(request, ctx, bulkService, lvm)
			if err != nil {
				return 0, err
			}
		}
	} else {
		for _, request := range requests {
			err := s.endpoint.parseByRegular(request, ctx, bulkService)
			if err != nil {
				log.Errorf(errors.ErrorStack(err))
				return 0, err
			}
		}
	}

	if bulkService.NumberOfActions() == 0 {
		return 0, nil
	}

	result, err := bulkService.Do(context.Background())
	if err != nil {
		return 0, err
	}

	return int64(len(result.Succeeded())), nil
}

func (s *BatchEndpoint) Connect() error {
	return s.endpoint.Connect()
}

func (s *BatchEndpoint) Ping() error {
	return s.endpoint.Ping()
}

func (s *BatchEndpoint) Close() {
	s.endpoint.Close()
}

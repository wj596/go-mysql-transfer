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

package endpoint

import (
	"github.com/yuin/gopher-lua"

	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/endpoint/redis"
)

type IEndpoint interface {
	Connect() error
	Ping() error
	Close()
}

type IStreamEndpoint interface {
	Connect() error
	Ping() error
	Close()
	Stream(requests []*bo.RowEventRequest) error
}

type IBatchEndpoint interface {
	Connect() error
	Ping() error
	Close()
	Batch(requests []*bo.RowEventRequest, ctx *bo.RuleContext, lvm *lua.LState) (int64, error)
}

func NewEndpoint(info *po.EndpointInfo) IEndpoint {
	if info.Type == constants.EndpointTypeRedis {
		return redis.NewEndpoint(info)
	}
	return nil
}

func NewStreamEndpoint(info *po.EndpointInfo) IStreamEndpoint {
	if info.Type == constants.EndpointTypeRedis {
		return redis.NewStreamEndpoint(info)
	}
	return nil
}

func NewBatchEndpoint(info *po.EndpointInfo) IBatchEndpoint {
	if info.Type == constants.EndpointTypeRedis {
		return redis.NewBatchEndpoint(info)
	}
	return nil
}

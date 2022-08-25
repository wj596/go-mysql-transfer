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
	"sync"

	"github.com/go-redis/redis"
	"github.com/juju/errors"
	"github.com/yuin/gopher-lua"

	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/util/log"
)

type BatchEndpoint struct {
	parent          *Endpoint
	pipelines       map[string]redis.Pipeliner
	lockOfPipelines sync.RWMutex
}

func NewBatchEndpoint(info *po.EndpointInfo) *BatchEndpoint {
	return &BatchEndpoint{
		parent:    NewEndpoint(info),
		pipelines: make(map[string]redis.Pipeliner),
	}
}

func (s *BatchEndpoint) Batch(requests []*bo.RowEventRequest, ctx *bo.RuleContext, lvm *lua.LState) (int64, error) {
	if ctx.IsLuaEnable() {
		for _, request := range requests {
			pipeline := s.getPipeline(ctx.GetTableFullName())
			err := s.parent.parseByLua(request, ctx, pipeline, lvm)
			if err != nil {
				return 0, err
			}
		}
	} else {
		for _, request := range requests {
			pipeline := s.getPipeline(ctx.GetTableFullName())
			err := s.parent.parseByRegular(request, ctx, pipeline)
			if err != nil {
				log.Errorf(errors.ErrorStack(err))
				return 0, err
			}
		}
	}

	var counter int64
	results, err := s.getPipeline(ctx.GetTableFullName()).Exec()

	if err != nil {
		log.Errorf(errors.ErrorStack(err))
		return 0, err
	}

	for _, result := range results {
		if result.Err() == nil {
			counter++
		} else {
			log.Error(result.Err().Error())
		}
	}

	return counter, err
}

func (s *BatchEndpoint) getPipeline(tableName string) redis.Pipeliner {
	s.lockOfPipelines.RLock()
	pipe, ok := s.pipelines[tableName]
	s.lockOfPipelines.RUnlock()
	if ok {
		return pipe
	}

	s.lockOfPipelines.Lock()
	defer s.lockOfPipelines.Unlock()
	pipe, ok = s.pipelines[tableName]
	if ok {
		return pipe
	}
	pipe = s.parent.createPipeline()
	s.pipelines[tableName] = pipe

	return pipe
}

func (s *BatchEndpoint) Connect() error {
	return s.parent.Connect()
}

func (s *BatchEndpoint) Ping() error {
	return s.parent.Ping()
}

func (s *BatchEndpoint) Close() {
	for _, pipe := range s.pipelines {
		pipe.Close()
	}
	s.parent.Close()
}

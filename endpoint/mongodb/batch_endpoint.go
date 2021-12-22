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

package mongodb

import (
	"context"

	"github.com/juju/errors"
	"github.com/yuin/gopher-lua"
	"go.mongodb.org/mongo-driver/mongo"

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
			info:        info,
			collections: make(map[string]*mongo.Collection),
		},
	}
}

func (s *BatchEndpoint) Batch(requests []*bo.RowEventRequest, ctx *bo.RuleContext, lvm *lua.LState) (int64, error) {
	models := make(map[string][]mongo.WriteModel, 0)

	if ctx.IsLuaEnable() {
		for _, request := range requests {
			err := s.endpoint.parseByLua(request, ctx, models, lvm)
			if err != nil {
				return 0, err
			}
		}
	} else {
		for _, request := range requests {
			err := s.endpoint.parseByRegular(request, ctx, models)
			if err != nil {
				return 0, err
			}
		}
	}

	var sum int64
	hasDuplicateKey := false
	for collectionName, array := range models {
		collection := s.endpoint.getCollection(collectionName)
		if nil == collection {
			return 0, errors.New("collection为空")
		}

		result, err := collection.BulkWrite(context.Background(), array)
		if err != nil {
			if s.endpoint.isDuplicateKeyError(err.Error()) {
				log.Warnf(err.Error())
				hasDuplicateKey = true
				break
			} else {
				return 0, err
			}
		}
		sum = sum + (result.InsertedCount + result.ModifiedCount + result.UpsertedCount + result.DeletedCount)
	}

	if hasDuplicateKey {
		nub, err := s.endpoint.doSlowly(models)
		if err != nil {
			log.Warnf(err.Error())
		}
		return nub, nil
	}

	return sum, nil
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

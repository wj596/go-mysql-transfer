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
	"strings"
	"sync"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/yuin/gopher-lua"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/endpoint/luaengine"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/stringutils"
)

type Endpoint struct {
	info              *po.EndpointInfo
	client            *mongo.Client
	options           *options.ClientOptions
	collections       map[string]*mongo.Collection
	lockOfCollections sync.RWMutex
}

func NewEndpoint(info *po.EndpointInfo) *Endpoint {
	return &Endpoint{
		info:        info,
		collections: make(map[string]*mongo.Collection),
	}
}

func (s *Endpoint) Connect() error {
	addrs := strings.Split(s.info.GetAddresses(), stringutils.Comma)
	opts := &options.ClientOptions{
		Hosts: addrs,
	}

	if s.info.GetUsername() != "" && s.info.GetPassword() != "" {
		opts.Auth = &options.Credential{
			Username: s.info.GetUsername(),
			Password: s.info.GetPassword(),
		}
	}

	client, err := mongo.Connect(context.Background(), s.options)
	if err != nil {
		return err
	}

	err = s.Ping()
	if err != nil {
		return err
	}

	s.client = client
	s.options = opts

	return nil
}

func (s *Endpoint) Ping() error {
	return s.client.Ping(context.Background(), readpref.Primary())
}

func (s *Endpoint) Close() {
	if s.client != nil {
		s.client.Disconnect(context.Background())
	}
	s.client = nil
	s.options = nil
}

func (s *Endpoint) getCollection(fullName string) *mongo.Collection {
	s.lockOfCollections.RLock()
	collection, exist := s.collections[fullName]
	s.lockOfCollections.RUnlock()
	if exist {
		return collection
	}
	return nil
}

func (s *Endpoint) getOrCreateCollection(ctx *bo.RuleContext) *mongo.Collection {
	s.lockOfCollections.RLock()
	collection, exist := s.collections[ctx.GetMongodbCollectionFullName()]
	s.lockOfCollections.RUnlock()
	if exist {
		return collection
	}

	s.lockOfCollections.Lock()
	collection = s.client.Database(ctx.GetRule().GetMongodbDatabase()).Collection(ctx.GetRule().GetMongodbCollection())
	s.collections[ctx.GetMongodbCollectionFullName()] = collection
	s.lockOfCollections.Unlock()

	return collection
}

func (s *Endpoint) isDuplicateKeyError(stack string) bool {
	return strings.Contains(stack, "E11000 duplicate key error")
}

func (s *Endpoint) parseByRegular(request *bo.RowEventRequest, ctx *bo.RuleContext, models map[string][]mongo.WriteModel) error {
	row := ctx.GetRow(request)
	id := ctx.GetPrimaryKeyValue(request)
	row["_id"] = id

	var model mongo.WriteModel
	switch request.Action {
	case canal.InsertAction:
		model = mongo.NewInsertOneModel().SetDocument(row)
	case canal.UpdateAction:
		model = mongo.NewUpdateOneModel().SetFilter(bson.M{"_id": id}).SetUpdate(bson.M{"$set": row})
	case canal.DeleteAction:
		model = mongo.NewDeleteOneModel().SetFilter(bson.M{"_id": id})
	}

	array, ok := models[ctx.GetMongodbCollectionFullName()]
	if !ok {
		array = make([]mongo.WriteModel, 0)
	}

	array = append(array, model)
	models[ctx.GetMongodbCollectionFullName()] = array

	return nil
}

func (s *Endpoint) parseByLua(request *bo.RowEventRequest, ctx *bo.RuleContext, models map[string][]mongo.WriteModel, lvm *lua.LState) error {
	var L *lua.LState
	if lvm != nil {
		L = lvm
	} else {
		L = ctx.GetLuaVM()
	}

	event := L.NewTable()
	row := L.NewTable()
	luaengine.PaddingLTable(L, row, ctx.GetRow(request))
	L.SetTable(event, luaengine.RowKey, row)
	if canal.UpdateAction == request.Action {
		preRow := L.NewTable()
		luaengine.PaddingLTable(L, preRow, ctx.GetPreRow(request))
		L.SetTable(event, luaengine.PreRowKey, preRow)
	}
	L.SetTable(event, luaengine.ActionKey, lua.LString(request.Action))

	result := L.NewTable()
	L.SetGlobal(luaengine.GlobalVariableResult, result)

	err := L.CallByParam(lua.P{
		Fn:      L.GetGlobal(luaengine.HandleFunctionName),
		NRet:    0,
		Protect: true,
	}, event)
	if err != nil {
		log.Errorf("管道[%s]，表[%s]的Lua脚本执行错误[%s]", ctx.GetPipelineName(), ctx.GetTableFullName(), err.Error)
		return constants.LuaScriptError
	}

	result.ForEach(func(k lua.LValue, v lua.LValue) {
		combine := luaengine.LvToString(k)
		clen := len(combine)
		action := combine[19:25]
		collection := combine[26:clen]
		collectionFullName := stringutils.Join(ctx.GetRule().GetMongodbDatabase(), collection)

		array, ok := models[collectionFullName]
		if !ok {
			array = make([]mongo.WriteModel, 0)
		}

		var model mongo.WriteModel
		switch action {
		case canal.InsertAction:
			var table map[string]interface{}
			table, err = luaengine.LvToMap(v)
			if err != nil {
				return
			}
			if _, exist := table["_id"]; !exist {
				pkv := ctx.GetPrimaryKeyValue(request)
				if nil != pkv {
					table["_id"] = pkv
				}
			}
			model = mongo.NewInsertOneModel().SetDocument(table)
		case canal.UpdateAction:
			var table map[string]interface{}
			table, err = luaengine.LvToMap(v)
			if err != nil {
				return
			}

			id := luaengine.LvToInterface(L.GetTable(v, lua.LString("id")), true)
			if _, exist := table["_id"]; !exist {
				table["_id"] = id
			}
			model = mongo.NewUpdateOneModel().SetFilter(bson.M{"_id": id}).SetUpdate(bson.M{"$set": table})
		case constants.UpsertAction:
			var table map[string]interface{}
			table, err = luaengine.LvToMap(v)
			if err != nil {
				return
			}

			id := luaengine.LvToInterface(L.GetTable(v, lua.LString("id")), true)
			if _, exist := table["_id"]; !exist {
				table["_id"] = id
			}
			model = mongo.NewUpdateOneModel().SetFilter(bson.M{"_id": id}).SetUpsert(true).SetUpdate(bson.M{"$set": table})
		case canal.DeleteAction:
			id := luaengine.LvToInterface(L.GetTable(v, lua.LString("id")), true)
			model = mongo.NewDeleteOneModel().SetFilter(bson.M{"_id": id})
		}

		array = append(array, model)
		models[collectionFullName] = array
		log.Infof("管道[%s] 接收端[mongodb]、事件[%s]、collection[%s]", ctx.GetPipelineName(), action, collection)
	})

	if err != nil {
		log.Errorf("管道[%s]，表[%s]的Lua脚本执行错误[%s]", ctx.GetPipelineName(), ctx.GetTableFullName(), err.Error)
		return constants.LuaScriptError
	}

	return nil
}

func (s *Endpoint) doSlowly(models map[string][]mongo.WriteModel) (int64, error) {
	var sum int64
	for collectionName, array := range models {
		collection := s.getCollection(collectionName)
		if nil == collection {
			return 0, errors.New("collection为空")
		}

		for _, item := range array {
			switch item.(type) {
			case *mongo.InsertOneModel:
				vv := item.(*mongo.InsertOneModel)
				_, err := collection.InsertOne(context.Background(), vv.Document)
				if err != nil {
					if s.isDuplicateKeyError(err.Error()) {
						log.Warn(err.Error())
					} else {
						return sum, err
					}
				}
			case *mongo.UpdateOneModel:
				vv := item.(*mongo.UpdateOneModel)
				upsert := &options.UpdateOptions{Upsert: vv.Upsert}
				_, err := collection.UpdateOne(context.Background(), vv.Filter, vv.Update, upsert)
				if err != nil {
					return sum, err
				}
			case *mongo.DeleteOneModel:
				vv := item.(*mongo.DeleteOneModel)
				_, err := collection.DeleteOne(context.Background(), vv.Filter)
				if err != nil {
					return sum, err
				}
			}
			sum++
		}
	}

	return sum, nil
}

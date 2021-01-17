/*
 * Copyright 2020-2021 the original author(https://github.com/wj596)
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
	"context"
	"log"
	"strings"
	"sync"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"go-mysql-transfer/global"
	"go-mysql-transfer/metrics"
	"go-mysql-transfer/model"
	"go-mysql-transfer/service/luaengine"
	"go-mysql-transfer/util/logs"
	"go-mysql-transfer/util/stringutil"
)

type cKey struct {
	database   string
	collection string
}

type MongoEndpoint struct {
	options     *options.ClientOptions
	client      *mongo.Client
	lock        sync.Mutex
	collections map[cKey]*mongo.Collection
	collLock    sync.RWMutex

	retryLock sync.Mutex
}

func newMongoEndpoint() *MongoEndpoint {
	addrList := strings.Split(global.Cfg().MongodbAddr, ",")
	opts := &options.ClientOptions{
		Hosts: addrList,
	}

	if global.Cfg().MongodbUsername != "" && global.Cfg().MongodbPassword != "" {
		opts.Auth = &options.Credential{
			Username: global.Cfg().MongodbUsername,
			Password: global.Cfg().MongodbPassword,
		}
	}

	r := &MongoEndpoint{}
	r.options = opts
	r.collections = make(map[cKey]*mongo.Collection)
	return r
}

func (s *MongoEndpoint) Connect() error {
	client, err := mongo.Connect(context.Background(), s.options)
	if err != nil {
		return err
	}

	s.client = client

	s.collLock.Lock()
	for _, rule := range global.RuleInsList() {
		cc := s.client.Database(rule.MongodbDatabase).Collection(rule.MongodbCollection)
		s.collections[s.collectionKey(rule.MongodbDatabase, rule.MongodbCollection)] = cc
	}
	s.collLock.Unlock()

	return nil
}

func (s *MongoEndpoint) Ping() error {
	return s.client.Ping(context.Background(), readpref.Primary())
}

func (s *MongoEndpoint) isDuplicateKeyError(stack string) bool {
	return strings.Contains(stack, "E11000 duplicate key error")
}

func (s *MongoEndpoint) collectionKey(database, collection string) cKey {
	return cKey{
		database:   database,
		collection: collection,
	}
}

func (s *MongoEndpoint) collection(key cKey) *mongo.Collection {
	s.collLock.RLock()
	c, ok := s.collections[key]
	s.collLock.RUnlock()
	if ok {
		return c
	}

	s.collLock.Lock()
	c = s.client.Database(key.database).Collection(key.collection)
	s.collections[key] = c
	s.collLock.Unlock()

	return c
}

func (s *MongoEndpoint) Consume(from mysql.Position, rows []*model.RowRequest) error {
	models := make(map[cKey][]mongo.WriteModel, 0)
	for _, row := range rows {
		rule, _ := global.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		metrics.UpdateActionNum(row.Action, row.RuleKey)

		if rule.LuaEnable() {
			kvm := rowMap(row, rule, true)
			ls, err := luaengine.DoMongoOps(kvm, row.Action, rule)
			if err != nil {
				return errors.Errorf("lua 脚本执行失败 : %s ", errors.ErrorStack(err))
			}
			for _, resp := range ls {
				var model mongo.WriteModel
				switch resp.Action {
				case canal.InsertAction:
					model = mongo.NewInsertOneModel().SetDocument(resp.Table)
				case canal.UpdateAction:
					model = mongo.NewUpdateOneModel().SetFilter(bson.M{"_id": resp.Id}).SetUpdate(bson.M{"$set": resp.Table})
				case global.UpsertAction:
					model = mongo.NewUpdateOneModel().SetFilter(bson.M{"_id": resp.Id}).SetUpsert(true).SetUpdate(bson.M{"$set": resp.Table})
				case canal.DeleteAction:
					model = mongo.NewDeleteOneModel().SetFilter(bson.M{"_id": resp.Id})
				}

				key := s.collectionKey(rule.MongodbDatabase, resp.Collection)
				array, ok := models[key]
				if !ok {
					array = make([]mongo.WriteModel, 0)
				}

				logs.Infof("action:%s, collection:%s, id:%v, data:%v", resp.Action, resp.Collection, resp.Id, resp.Table)

				array = append(array, model)
				models[key] = array
			}
		} else {
			kvm := rowMap(row, rule, false)
			id := primaryKey(row, rule)
			kvm["_id"] = id
			var model mongo.WriteModel
			switch row.Action {
			case canal.InsertAction:
				model = mongo.NewInsertOneModel().SetDocument(kvm)
			case canal.UpdateAction:
				model = mongo.NewUpdateOneModel().SetFilter(bson.M{"_id": id}).SetUpdate(bson.M{"$set": kvm})
			case canal.DeleteAction:
				model = mongo.NewDeleteOneModel().SetFilter(bson.M{"_id": id})
			}

			ccKey := s.collectionKey(rule.MongodbDatabase, rule.MongodbCollection)
			array, ok := models[ccKey]
			if !ok {
				array = make([]mongo.WriteModel, 0)
			}

			logs.Infof("action:%s, collection:%s, id:%v, data:%v", row.Action, rule.MongodbCollection, id, kvm)

			array = append(array, model)
			models[ccKey] = array
		}
	}

	var slowly bool
	for key, model := range models {
		collection := s.collection(key)
		_, err := collection.BulkWrite(context.Background(), model)
		if err != nil {
			if s.isDuplicateKeyError(err.Error()) {
				slowly = true
			} else {
				return err
			}
			logs.Error(errors.ErrorStack(err))
			break
		}
	}
	if slowly {
		_, err := s.doConsumeSlowly(rows)
		if err != nil {
			return err
		}
	}

	logs.Infof("处理完成 %d 条数据", len(rows))
	return nil
}

func (s *MongoEndpoint) Stock(rows []*model.RowRequest) int64 {
	expect := true
	models := make(map[cKey][]mongo.WriteModel, 0)
	for _, row := range rows {
		rule, _ := global.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		if rule.LuaEnable() {
			kvm := rowMap(row, rule, true)
			ls, err := luaengine.DoMongoOps(kvm, row.Action, rule)
			if err != nil {
				log.Println("Lua 脚本执行失败!!! ,详情请参见日志")
				logs.Errorf("lua 脚本执行失败 : %s ", errors.ErrorStack(err))
				expect = false
				break
			}

			for _, resp := range ls {
				ccKey := s.collectionKey(rule.MongodbDatabase, resp.Collection)
				model := mongo.NewInsertOneModel().SetDocument(resp.Table)
				array, ok := models[ccKey]
				if !ok {
					array = make([]mongo.WriteModel, 0)
				}
				array = append(array, model)
				models[ccKey] = array
			}
		} else {
			kvm := rowMap(row, rule, false)
			id := primaryKey(row, rule)
			kvm["_id"] = id

			ccKey := s.collectionKey(rule.MongodbDatabase, rule.MongodbCollection)
			model := mongo.NewInsertOneModel().SetDocument(kvm)
			array, ok := models[ccKey]
			if !ok {
				array = make([]mongo.WriteModel, 0)
			}
			array = append(array, model)
			models[ccKey] = array
		}
	}

	if !expect {
		return 0
	}

	var slowly bool
	var sum int64
	for key, vs := range models {
		collection := s.collection(key)
		rr, err := collection.BulkWrite(context.Background(), vs)
		if err != nil {
			if s.isDuplicateKeyError(err.Error()) {
				slowly = true
			}
			logs.Error(errors.ErrorStack(err))
			break
		}
		sum += rr.InsertedCount
	}

	if slowly {
		logs.Info("do consume slowly ... ... ")
		slowlySum, err := s.doConsumeSlowly(rows)
		if err != nil {
			logs.Warnf(err.Error())
		}
		return slowlySum
	}

	return sum
}

func (s *MongoEndpoint) doConsumeSlowly(rows []*model.RowRequest) (int64, error) {
	var sum int64
	for _, row := range rows {
		rule, _ := global.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		if rule.LuaEnable() {
			kvm := rowMap(row, rule, true)
			ls, err := luaengine.DoMongoOps(kvm, row.Action, rule)
			if err != nil {
				logs.Errorf("lua 脚本执行失败 : %s ", errors.ErrorStack(err))
				return sum, err
			}
			for _, resp := range ls {
				collection := s.collection(s.collectionKey(rule.MongodbDatabase, resp.Collection))
				switch resp.Action {
				case canal.InsertAction:
					_, err := collection.InsertOne(context.Background(), resp.Table)
					if err != nil {
						if s.isDuplicateKeyError(err.Error()) {
							logs.Warnf("duplicate key [ %v ]", stringutil.ToJsonString(resp.Table))
						} else {
							return sum, err
						}
					}
				case canal.UpdateAction:
					_, err := collection.UpdateOne(context.Background(), bson.M{"_id": resp.Id}, bson.M{"$set": resp.Table})
					if err != nil {
						return sum, err
					}
				case canal.DeleteAction:
					_, err := collection.DeleteOne(context.Background(), bson.M{"_id": resp.Id})
					if err != nil {
						return sum, err
					}
				}
				logs.Infof("action:%s, collection:%s, id:%v, data:%v",
					row.Action, collection.Name(), resp.Id, resp.Table)
			}
		} else {
			kvm := rowMap(row, rule, false)
			id := primaryKey(row, rule)
			kvm["_id"] = id

			collection := s.collection(s.collectionKey(rule.MongodbDatabase, rule.MongodbCollection))

			switch row.Action {
			case canal.InsertAction:
				_, err := collection.InsertOne(context.Background(), kvm)
				if err != nil {
					if s.isDuplicateKeyError(err.Error()) {
						logs.Warnf("duplicate key [ %v ]", stringutil.ToJsonString(kvm))
					} else {
						return sum, err
					}
				}
			case canal.UpdateAction:
				_, err := collection.UpdateOne(context.Background(), bson.M{"_id": id}, bson.M{"$set": kvm})
				if err != nil {
					return sum, err
				}
			case canal.DeleteAction:
				_, err := collection.DeleteOne(context.Background(), bson.M{"_id": id})
				if err != nil {
					return sum, err
				}
			}

			logs.Infof("action:%s, collection:%s, id:%v, data:%v", row.Action, collection.Name(), id, kvm)
		}
		sum++
	}
	return sum, nil
}

func (s *MongoEndpoint) Close() {
	if s.client != nil {
		s.client.Disconnect(context.Background())
	}
}

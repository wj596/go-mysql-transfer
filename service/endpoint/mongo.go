package endpoint

import (
	"context"
	"strings"
	"sync"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/vmihailenco/msgpack"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"go-mysql-transfer/global"
	"go-mysql-transfer/service/luaengine"
	"go-mysql-transfer/storage"
	"go-mysql-transfer/util/logutil"
	"go-mysql-transfer/util/stringutil"
)

type MongoEndpoint struct {
	config *global.Config
	cached *storage.BoltRowStorage

	options     *options.ClientOptions
	client      *mongo.Client
	lock        sync.Mutex
	collections map[string]*mongo.Collection
}

func newMongoEndpoint(c *global.Config) *MongoEndpoint {
	addrList := strings.Split(c.MongodbAddr, ",")
	opts := &options.ClientOptions{
		Hosts: addrList,
	}

	if c.MongodbUsername != "" && c.MongodbPassword != "" {
		opts.Auth = &options.Credential{
			Username: c.MongodbUsername,
			Password: c.MongodbPassword,
		}
	}

	r := &MongoEndpoint{}
	r.config = c
	r.cached = &storage.BoltRowStorage{}
	r.options = opts
	r.collections = make(map[string]*mongo.Collection)

	return r
}

func (s *MongoEndpoint) Start() error {
	client, err := mongo.Connect(context.Background(), s.options)
	if err != nil {
		return err
	}

	s.client = client

	for _, rule := range global.RuleInsList() {
		cc := s.client.Database(rule.MongodbDatabase).Collection(rule.MongodbCollection)
		s.collections[s.collectionKey(rule.MongodbDatabase, rule.MongodbCollection)] = cc
	}

	return nil
}

func (s *MongoEndpoint) Ping() error {
	return s.client.Ping(context.Background(), readpref.Primary())
}

func (s *MongoEndpoint) isDuplicateKeyError(stack string) bool {
	return strings.Contains(stack, "E11000 duplicate key error")
}

func (s *MongoEndpoint) collectionKey(database, collection string) string {
	return database + "-*|*-" + collection
}

func (s *MongoEndpoint) collection(key string) *mongo.Collection {
	collection, exist := s.collections[key]
	if !exist {
		index := strings.Index(key, "-*|*-")
		db := key[0:index]
		cc := key[index+5 : len(key)]
		collection = s.client.Database(db).Collection(cc)
		s.collections[key] = collection
	}
	return collection
}

func (s *MongoEndpoint) Consume(rows []*global.RowRequest) {
	if err := s.doRetryTask(); err != nil {
		logutil.Error(err.Error())
		pushFailedRows(rows, s.cached)
		return
	}

	expect := true
	models := make(map[string][]mongo.WriteModel, 0)
	for _, row := range rows {
		rule, _ := global.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logutil.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		exportActionNum(row.Action, row.RuleKey)

		if rule.LuaNecessary() {
			kvm := keyValueMap(row, rule, true)
			ls, err := luaengine.DoMongoOps(kvm, row.Action, rule)
			if err != nil {
				logutil.Errorf("lua 脚本执行失败 : %s ", errors.ErrorStack(err))
				expect = false
				break
			}

			for _, resp := range ls {
				var model mongo.WriteModel
				switch resp.Action {
				case canal.InsertAction:
					model = mongo.NewInsertOneModel().SetDocument(resp.Table)
				case canal.UpdateAction:
					model = mongo.NewUpdateOneModel().SetFilter(bson.M{"_id": resp.Id}).SetUpdate(bson.M{"$set": resp.Table})
				case canal.DeleteAction:
					model = mongo.NewDeleteOneModel().SetFilter(bson.M{"_id": resp.Id})
				}

				ccKey := s.collectionKey(rule.MongodbDatabase, resp.Collection)
				array, ok := models[ccKey]
				if !ok {
					array = make([]mongo.WriteModel, 0)
				}

				logutil.Infof("action:%s, collection:%s, id:%v, data:%v", resp.Action, resp.Collection, resp.Id, resp.Table)

				array = append(array, model)
				models[ccKey] = array
			}
		} else {
			kvm := keyValueMap(row, rule, false)
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

			logutil.Infof("action:%s, collection:%s, id:%v, data:%v", row.Action, rule.MongodbCollection, id, kvm)

			array = append(array, model)
			models[ccKey] = array
		}
	}

	if !expect {
		pushFailedRows(rows, s.cached)
		logutil.Infof("%d 条数据处理失败，插入重试队列", len(rows))
		return
	}

	var slowly bool
	for key, model := range models {
		collection := s.collection(key)
		_, err := collection.BulkWrite(context.Background(), model)
		if err != nil {
			if s.isDuplicateKeyError(err.Error()) {
				slowly = true
			} else {
				expect = false
			}
			logutil.Error(errors.ErrorStack(err))
			break
		}
	}

	if slowly {
		_, err := s.doConsumeSlowly(rows)
		if err != nil {
			logutil.Warnf(err.Error())
			expect = false
		}
	}

	if !expect {
		pushFailedRows(rows, s.cached)
		logutil.Infof("%d 条数据处理失败，插入重试队列", len(rows))
	} else {
		logutil.Infof("处理完成 %d 条数据", len(rows))
	}
}

func (s *MongoEndpoint) Stock(rows []*global.RowRequest) int64 {
	expect := true
	models := make(map[string][]mongo.WriteModel, 0)
	for _, row := range rows {
		rule, _ := global.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logutil.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		if rule.LuaNecessary() {
			kvm := keyValueMap(row, rule, true)
			ls, err := luaengine.DoMongoOps(kvm, row.Action, rule)
			if err != nil {
				logutil.Errorf("lua 脚本执行失败 : %s ", errors.ErrorStack(err))
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
			kvm := keyValueMap(row, rule, false)
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
			logutil.Error(errors.ErrorStack(err))
			break
		}
		sum += rr.InsertedCount
	}

	if slowly {
		logutil.Info("do consume slowly ... ... ")
		slowlySum, err := s.doConsumeSlowly(rows)
		if err != nil {
			logutil.Warnf(err.Error())
		}
		return slowlySum
	}

	return sum
}

func (s *MongoEndpoint) doConsumeSlowly(rows []*global.RowRequest) (int64, error) {
	var sum int64
	for _, row := range rows {
		rule, _ := global.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logutil.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		if rule.LuaNecessary() {
			kvm := keyValueMap(row, rule, true)
			ls, err := luaengine.DoMongoOps(kvm, row.Action, rule)
			if err != nil {
				logutil.Errorf("lua 脚本执行失败 : %s ", errors.ErrorStack(err))
				return sum, err
			}
			for _, resp := range ls {
				collection := s.collection(s.collectionKey(rule.MongodbDatabase, resp.Collection))
				switch resp.Action {
				case canal.InsertAction:
					_, err := collection.InsertOne(context.Background(), resp.Table)
					if err != nil {
						if s.isDuplicateKeyError(err.Error()) {
							logutil.Warnf("duplicate key [ %v ]", stringutil.ToJsonString(resp.Table))
						} else {
							return sum, err
						}
					}
				case canal.UpdateAction:
					_, err := collection.UpdateOne(context.Background(), bson.M{"_id": resp.Id}, resp.Table)
					if err != nil {
						return sum, err
					}
				case canal.DeleteAction:
					_, err := collection.DeleteOne(context.Background(), bson.M{"_id": resp.Id})
					if err != nil {
						return sum, err
					}
				}
				logutil.Infof("action:%s, collection:%s, id:%v, data:%v",
					row.Action, collection.Name(), resp.Id, resp.Table)
			}
		} else {
			kvm := keyValueMap(row, rule, false)
			id := primaryKey(row, rule)
			kvm["_id"] = id

			collection := s.collection(s.collectionKey(rule.MongodbDatabase, rule.MongodbCollection))

			switch row.Action {
			case canal.InsertAction:
				_, err := collection.InsertOne(context.Background(), kvm)
				if err != nil {
					if s.isDuplicateKeyError(err.Error()) {
						logutil.Warnf("duplicate key [ %v ]", stringutil.ToJsonString(kvm))
					} else {
						return sum, err
					}
				}
			case canal.UpdateAction:
				_, err := collection.UpdateOne(context.Background(), bson.M{"_id": id}, kvm)
				if err != nil {
					return sum, err
				}
			case canal.DeleteAction:
				_, err := collection.DeleteOne(context.Background(), bson.M{"_id": id})
				if err != nil {
					return sum, err
				}
			}

			logutil.Infof("action:%s, collection:%s, id:%v, data:%v", row.Action, collection.Name(), id, kvm)
		}
		sum++
	}
	return sum, nil
}

func (s *MongoEndpoint) doRetryTask() error {
	if s.cached.Size() == 0 {
		return nil
	}

	if err := s.Ping(); err != nil {
		return err
	}

	logutil.Infof("当前重试队列有%d 条数据", s.cached.Size())

	var data []byte
	ids := s.cached.IdList()
	for _, id := range ids {
		var err error
		data, err = s.cached.Get(id)
		if err != nil {
			logutil.Warn(err.Error())
			s.cached.Delete(id)
			continue
		}

		var row global.RowRequest
		err = msgpack.Unmarshal(data, &row)
		if err != nil {
			logutil.Warn(err.Error())
			s.cached.Delete(id)
			continue
		}

		rows := []*global.RowRequest{&row}
		_, err = s.doConsumeSlowly(rows)
		if err != nil {
			return err
		}

		logutil.Infof("cached id :%d , 数据重试成功", id)
		s.cached.Delete(id)
	}

	return nil
}

func (s *MongoEndpoint) Close() {
	if s.client != nil {
		s.client.Disconnect(context.Background())
	}
}

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
	"github.com/olivere/elastic"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"

	"go-mysql-transfer/global"
	"go-mysql-transfer/metrics"
	"go-mysql-transfer/model"
	"go-mysql-transfer/service/luaengine"
	"go-mysql-transfer/util/logagent"
	"go-mysql-transfer/util/logs"
	"go-mysql-transfer/util/stringutil"
)

type Elastic6Endpoint struct {
	first  string
	hosts  []string
	client *elastic.Client

	retryLock sync.Mutex
}

func newElastic6Endpoint() *Elastic6Endpoint {
	r := &Elastic6Endpoint{}
	r.hosts = strings.Split(global.Cfg().ElsAddr, ",")
	r.first = r.hosts[0]
	return r
}

func (s *Elastic6Endpoint) Connect() error {
	var options []elastic.ClientOptionFunc
	options = append(options, elastic.SetErrorLog(logagent.NewElsLoggerAgent()))
	options = append(options, elastic.SetURL(s.hosts...))
	if global.Cfg().ElsUser != "" && global.Cfg().ElsPassword != "" {
		options = append(options, elastic.SetBasicAuth(global.Cfg().ElsUser, global.Cfg().Password))
	}

	client, err := elastic.NewClient(options...)
	if err != nil {
		return err
	}

	s.client = client
	return s.indexMapping()
}

func (s *Elastic6Endpoint) indexMapping() error {
	for _, rule := range global.RuleInsList() {
		exists, err := s.client.IndexExists(rule.ElsIndex).Do(context.Background())
		if err != nil {
			return err
		}
		if exists {
			err = s.updateIndexMapping(rule)
		} else {
			err = s.insertIndexMapping(rule)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Elastic6Endpoint) insertIndexMapping(rule *global.Rule) error {
	var properties map[string]interface{}
	if rule.LuaEnable() {
		properties = buildPropertiesByMappings(rule)
	} else {
		properties = buildPropertiesByRule(rule)
	}

	mapping := map[string]interface{}{
		"mappings": map[string]interface{}{
			rule.ElsType: map[string]interface{}{
				"properties": properties,
			},
		},
	}
	body := stringutil.ToJsonString(mapping)

	ret, err := s.client.CreateIndex(rule.ElsIndex).Body(body).Do(context.Background())
	if err != nil {
		return err
	}
	if !ret.Acknowledged {
		return errors.Errorf("create index %s err", rule.ElsIndex)
	}
	logs.Infof("create index succeed, index: %s", body)

	return nil
}

func (s *Elastic6Endpoint) updateIndexMapping(rule *global.Rule) error {
	ret, err := s.client.GetMapping().Index(rule.ElsIndex).Do(context.Background())
	if err != nil {
		return err
	}

	if ret[rule.ElsIndex]==nil{
		return nil
	}
	retIndex := ret[rule.ElsIndex].(map[string]interface{})

	if retIndex["mappings"] == nil {
		return nil
	}
	retMaps := retIndex["mappings"].(map[string]interface{})

	if retMaps["_doc"]==nil{
		return nil
	}
	retDoc := retMaps["_doc"].(map[string]interface{})

	if retDoc["properties"]==nil{
		return nil
	}
	retPros := retDoc["properties"].(map[string]interface{})

	var currents map[string]interface{}
	if rule.LuaEnable() {
		currents = buildPropertiesByMappings(rule)
	} else {
		currents = buildPropertiesByRule(rule)
	}

	if len(retPros) < len(currents) {
		properties := make(map[string]interface{})
		mapping := map[string]interface{}{
			"properties": properties,
		}
		for field, current := range currents {
			if _, exist := retPros[field]; !exist {
				properties[field] = current
			}
		}
		doc := stringutil.ToJsonString(mapping)
		ret, err := s.client.PutMapping().Index(rule.ElsIndex).Type(rule.ElsType).BodyString(doc).Do(context.Background())
		if err != nil {
			return err
		}
		if !ret.Acknowledged {
			return errors.Errorf("update index %s err", rule.ElsIndex)
		}
		logs.Infof("update index succeed, index: %s", doc)
	}

	return nil
}

func (s *Elastic6Endpoint) Ping() error {
	if _, _, err := s.client.Ping(s.first).Do(context.Background()); err == nil {
		return nil
	}

	for _, host := range s.hosts {
		if _, _, err := s.client.Ping(host).Do(context.Background()); err == nil {
			return nil
		}
	}

	return errors.New("ssx")
}

func (s *Elastic6Endpoint) Consume(from mysql.Position, rows []*model.RowRequest) error {
	bulk := s.client.Bulk()
	for _, row := range rows {
		rule, _ := global.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		metrics.UpdateActionNum(row.Action, row.RuleKey)

		if rule.LuaEnable() {
			kvm := rowMap(row, rule, true)
			ls, err := luaengine.DoESOps(kvm, row.Action, rule)
			if err != nil {
				log.Println("Lua 脚本执行失败!!! ,详情请参见日志")
				return errors.Errorf("lua 脚本执行失败 : %s ", errors.ErrorStack(err))
			}
			for _, resp := range ls {
				logs.Infof("action: %s, Index: %s , Id:%s, value: %v", resp.Action, resp.Index, resp.Id, resp.Date)
				s.prepareBulk(resp.Action, resp.Index, rule.ElsType, resp.Id, resp.Date, bulk)
			}
		} else {
			kvm := rowMap(row, rule, false)
			id := primaryKey(row, rule)
			body := encodeValue(rule, kvm)
			logs.Infof("action: %s, Index: %s , Id:%s, value: %v", row.Action, rule.ElsIndex, id, body)
			s.prepareBulk(row.Action, rule.ElsIndex, rule.ElsType, stringutil.ToString(id), body, bulk)
		}
	}

	if bulk.NumberOfActions()==0{
		return nil
	}

	r, err := bulk.Do(context.Background())
	if err != nil {
		log.Println(err.Error())
		return err
	}

	if len(r.Failed()) > 0 {
		for _, f := range r.Failed() {
			reason := f.Index + " " + f.Type + " " + f.Result
			if f.Error == nil && "not_found" == f.Result {
				return nil
			}

			if f.Error != nil {
				reason = f.Error.Reason
			}
			log.Println(reason)
			return errors.New(reason)
		}
	}

	logs.Infof("处理完成 %d 条数据", len(rows))
	return nil
}

func (s *Elastic6Endpoint) Stock(rows []*model.RowRequest) int64 {
	if len(rows) == 0 {
		return 0
	}

	bulk := s.client.Bulk()
	for _, row := range rows {
		rule, _ := global.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		if rule.LuaEnable() {
			kvm := rowMap(row, rule, true)
			ls, err := luaengine.DoESOps(kvm, row.Action, rule)
			if err != nil {
				logs.Errorf("lua 脚本执行失败 : %s ", errors.ErrorStack(err))
				break
			}
			for _, resp := range ls {
				s.prepareBulk(resp.Action, resp.Index, rule.ElsType, resp.Id, resp.Date, bulk)
			}
		} else {
			kvm := rowMap(row, rule, false)
			id := primaryKey(row, rule)
			body := encodeValue(rule, kvm)
			s.prepareBulk(row.Action, rule.ElsIndex, rule.ElsType, stringutil.ToString(id), body, bulk)
		}
	}

	r, err := bulk.Do(context.Background())
	if err != nil {
		logs.Error(errors.ErrorStack(err))
		return 0
	}

	return int64(len(r.Succeeded()))
}

func (s *Elastic6Endpoint) prepareBulk(action, index, _type, id, doc string, bulk *elastic.BulkService) {
	switch action {
	case canal.InsertAction:
		req := elastic.NewBulkIndexRequest().Index(index).Type(_type).Id(id).Doc(doc)
		bulk.Add(req)
	case canal.UpdateAction:
		req := elastic.NewBulkUpdateRequest().Index(index).Type(_type).Id(id).Doc(doc)
		bulk.Add(req)
	case canal.DeleteAction:
		req := elastic.NewBulkDeleteRequest().Index(index).Type(_type).Id(id)
		bulk.Add(req)
	}
	logs.Infof("index: %s, type:%s, action:%s, doc: %s", index, _type, action, doc)
}

func (s *Elastic6Endpoint) Close() {
	if s.client != nil {
		s.client.Stop()
	}
}

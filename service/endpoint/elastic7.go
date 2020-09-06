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
	"fmt"

	"github.com/juju/errors"
	"github.com/olivere/elastic/v7"
	"github.com/siddontang/go-mysql/canal"
	"github.com/vmihailenco/msgpack"

	"go-mysql-transfer/global"
	"go-mysql-transfer/service/luaengine"
	"go-mysql-transfer/storage"
	"go-mysql-transfer/util/logutil"
	"go-mysql-transfer/util/stringutil"
)

type Elastic7Endpoint struct {
	config *global.Config
	cached *storage.BoltRowStorage

	first  string
	hosts  []string
	client *elastic.Client
}

func newElastic7Endpoint(c *global.Config) *Elastic7Endpoint {
	hosts := elsHosts(c.ElsAddr)
	r := &Elastic7Endpoint{}
	r.config = c
	r.cached = &storage.BoltRowStorage{}
	r.hosts = hosts
	r.first = hosts[0]
	return r
}

func (s *Elastic7Endpoint) Start() error {
	var options []elastic.ClientOptionFunc
	options = append(options, elastic.SetErrorLog(logutil.NewElsLoggerAgent()))
	options = append(options, elastic.SetURL(s.hosts...))
	if s.config.ElsUser != "" && s.config.ElsPassword != "" {
		options = append(options, elastic.SetBasicAuth(s.config.ElsUser, s.config.Password))
	}

	client, err := elastic.NewClient(options...)
	if err != nil {
		return err
	}

	s.client = client
	return s.indexMapping()
}

func (s *Elastic7Endpoint) indexMapping() error {
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

func (s *Elastic7Endpoint) insertIndexMapping(rule *global.Rule) error {
	var properties map[string]interface{}
	if rule.LuaNecessary() {
		properties = buildPropertiesByMappings(rule)
	} else {
		properties = buildPropertiesByRule(rule)
	}

	mapping := map[string]interface{}{
		"mappings": map[string]interface{}{
			"properties": properties,
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

	logutil.Infof("create index: %s ,mappings: %s", rule.ElsIndex, body)

	return nil
}

func (s *Elastic7Endpoint) updateIndexMapping(rule *global.Rule) error {
	ret, err := s.client.GetMapping().Index(rule.ElsIndex).Do(context.Background())
	if err != nil {
		return err
	}
	retIndex := ret[rule.ElsIndex].(map[string]interface{})
	retMaps := retIndex["mappings"].(map[string]interface{})
	retPros := retMaps["properties"].(map[string]interface{})

	var currents map[string]interface{}
	if rule.LuaNecessary() {
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
		ret, err := s.client.PutMapping().Index(rule.ElsIndex).BodyString(doc).Do(context.Background())
		if err != nil {
			return err
		}
		if !ret.Acknowledged {
			return errors.Errorf("update index %s err", rule.ElsIndex)
		}

		logutil.Infof("create index: %s ,properties: %s", rule.ElsIndex, doc)
	}

	return nil
}

func (s *Elastic7Endpoint) Ping() error {
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

func (s *Elastic7Endpoint) Consume(rows []*global.RowRequest) {
	if err := s.doRetryTask(); err != nil {
		logutil.Error(err.Error())
		pushFailedRows(rows, s.cached)
		return
	}

	expect := true
	bulk := s.client.Bulk()
	for _, row := range rows {
		rule, _ := global.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logutil.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		exportActionNum(row.Action, row.RuleKey)

		if rule.LuaNecessary() {
			kvm := keyValueMap(row, rule, true)
			ls, err := luaengine.DoESOps(kvm, row.Action, rule)
			if err != nil {
				logutil.Errorf("lua 脚本执行失败 : %s ", errors.ErrorStack(err))
				expect = false
				break
			}
			for _, resp := range ls {
				logutil.Infof("action: %s, Index: %s , Id:%s, value: %v", resp.Action, resp.Index, resp.Id, resp.Date)
				s.prepareBulk(resp.Action, resp.Index, resp.Id, resp.Date, bulk)
			}
		} else {
			kvm := keyValueMap(row, rule, false)
			id := primaryKey(row, rule)
			body := encodeStringValue(rule, kvm)
			logutil.Infof("action: %s, Index: %s , Id:%s, value: %v", row.Action, rule.ElsIndex, id, body)
			s.prepareBulk(row.Action, rule.ElsIndex, stringutil.ToString(id), body, bulk)
		}

		global.RowRequestPool.Put(row)
	}

	r, err := bulk.Do(context.Background())
	if err != nil {
		logutil.Error(errors.ErrorStack(err))
		expect = false
	}

	if len(r.Failed()) > 0 {
		expect = false
		for _, f := range r.Failed() {
			logutil.Error(f.Error.Reason)
		}
	}

	if !expect {
		pushFailedRows(rows, s.cached)
	} else {
		logutil.Infof("处理完成 %d 条数据", len(rows))
	}
}

func (s *Elastic7Endpoint) Stock(rows []*global.RowRequest) int64 {
	bulk := s.client.Bulk()
	for _, row := range rows {
		rule, _ := global.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logutil.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		if rule.LuaNecessary() {
			kvm := keyValueMap(row, rule, true)
			ls, err := luaengine.DoESOps(kvm, row.Action, rule)
			if err != nil {
				logutil.Errorf("lua 脚本执行失败 : %s ", errors.ErrorStack(err))
				break
			}
			for _, resp := range ls {
				s.prepareBulk(resp.Action, resp.Index, resp.Id, resp.Date, bulk)
			}
		} else {
			kvm := keyValueMap(row, rule, false)
			id := primaryKey(row, rule)
			body := encodeStringValue(rule, kvm)
			fmt.Println(body)
			s.prepareBulk(row.Action, rule.ElsIndex, stringutil.ToString(id), body, bulk)
		}
	}

	if bulk.NumberOfActions() == 0 {
		return 0
	}

	r, err := bulk.Do(context.Background())
	if err != nil {
		logutil.Error(errors.ErrorStack(err))
	}

	if len(r.Failed()) > 0 {
		for _, f := range r.Failed() {
			fmt.Println(f.Error.Reason)
		}
	}

	return int64(len(r.Succeeded()))
}

func (s *Elastic7Endpoint) prepareBulk(action, index, id, doc string, bulk *elastic.BulkService) {
	switch action {
	case canal.InsertAction:
		req := elastic.NewBulkIndexRequest().Index(index).Id(id).Doc(doc)
		bulk.Add(req)
	case canal.UpdateAction:
		req := elastic.NewBulkUpdateRequest().Index(index).Id(id).Doc(doc)
		bulk.Add(req)
	case canal.DeleteAction:
		req := elastic.NewBulkDeleteRequest().Index(index).Id(id)
		bulk.Add(req)
	}

	logutil.Infof("index: %s, doc: %s", index, doc)
}

func (s *Elastic7Endpoint) doRequest(action, index, id, doc string) error {

	logutil.Infof("index: %s, action:%s, doc: %s", index, action, doc)
	switch action {
	case canal.InsertAction:
		_, err := s.client.Index().Index(index).Id(id).BodyString(doc).Do(context.Background())
		return err
	case canal.UpdateAction:
		_, err := s.client.Update().Index(index).Id(id).Doc(doc).Do(context.Background())
		return err
	case canal.DeleteAction:
		_, err := s.client.Delete().Index(index).Id(id).Do(context.Background())
		return err
	}

	return nil
}

func (s *Elastic7Endpoint) doRetryTask() error {
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

		rule, _ := global.RuleIns(row.RuleKey)
		if rule.LuaNecessary() {
			kvm := keyValueMap(&row, rule, true)
			ls, err := luaengine.DoESOps(kvm, row.Action, rule)
			if err != nil {
				return errors.Errorf("lua 脚本执行失败 : %s ", errors.ErrorStack(err))
			}
			for _, resp := range ls {
				err = s.doRequest(resp.Action, resp.Index, resp.Id, resp.Date)
				if err != nil {
					return err
				}
			}
		} else {
			kvm := keyValueMap(&row, rule, false)
			id := primaryKey(&row, rule)
			body := encodeStringValue(rule, kvm)
			err = s.doRequest(row.Action, rule.ElsIndex, stringutil.ToString(id), body)
			if err != nil {
				return err
			}
		}

		logutil.Infof("cached id :%d , 数据重试成功", id)
		s.cached.Delete(id)
	}

	return nil
}

func (s *Elastic7Endpoint) Close() {
	if s.client != nil {
		s.client.Stop()
	}
}

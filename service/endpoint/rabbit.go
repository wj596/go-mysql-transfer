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
	"github.com/siddontang/go-mysql/canal"
	"log"
	"strconv"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/streadway/amqp"

	"go-mysql-transfer/global"
	"go-mysql-transfer/metrics"
	"go-mysql-transfer/model"
	"go-mysql-transfer/service/luaengine"
	"go-mysql-transfer/util/logs"
	"go-mysql-transfer/util/nets"
)

type RabbitEndpoint struct {
	rabCon    *amqp.Connection
	rabChl    *amqp.Channel
	queues    map[string]bool
	serverUrl string
}

func newRabbitEndpoint() *RabbitEndpoint {
	r := &RabbitEndpoint{}
	r.queues = make(map[string]bool)
	return r
}

func (s *RabbitEndpoint) Connect() error {
	if s.rabChl != nil {
		s.rabChl.Close()
		s.rabChl = nil
	}
	if s.rabCon != nil {
		s.rabCon.Close()
		s.rabCon = nil
	}

	con, err := amqp.Dial(global.Cfg().RabbitmqAddr)
	if err != nil {
		return err
	}

	uri, _ := amqp.ParseURI(global.Cfg().RabbitmqAddr)
	s.serverUrl = uri.Host + ":" + strconv.Itoa(uri.Port)

	var chl *amqp.Channel
	chl, err = con.Channel()
	if err != nil {
		return err
	}

	s.rabCon = con
	s.rabChl = chl

	if len(s.queues) == 0 {
		for _, rule := range global.RuleInsList() {
			_, err := s.rabChl.QueueDeclare(
				rule.RabbitmqQueue, false, false, false, false, nil,
			)
			if err != nil {
				return err
			}
			s.queues[rule.RabbitmqQueue] = true
		}
	}

	return nil
}

func (s *RabbitEndpoint) Ping() error {
	_, err := nets.IsActiveTCPAddr(s.serverUrl)
	return err
}

func (s *RabbitEndpoint) mergeQueue(name string) {
	_, ok := s.queues[name]
	if ok {
		return
	}

	s.rabChl.QueueDeclare(name, false, false, false, false, nil)
	s.queues[name] = true
}

func (s *RabbitEndpoint) Consume(from mysql.Position, rows []*model.RowRequest) error {
	for _, row := range rows {
		rule, _ := global.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		metrics.UpdateActionNum(row.Action, row.RuleKey)

		if rule.LuaEnable() {
			err := s.doLuaConsume(row, rule)
			if err != nil {
				return err
			}
		} else {
			err := s.doRuleConsume(row, rule)
			if err != nil {
				return err
			}
		}
	}

	logs.Infof("处理完成 %d 条数据", len(rows))
	return nil
}

func (s *RabbitEndpoint) Stock(rows []*model.RowRequest) int64 {
	var sum int64
	for _, row := range rows {
		rule, _ := global.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		if rule.LuaEnable() {
			err := s.doLuaConsume(row, rule)
			if err != nil {
				logs.Errorf(errors.ErrorStack(err))
				break
			}
		} else {
			err := s.doRuleConsume(row, rule)
			if err != nil {
				logs.Errorf(errors.ErrorStack(err))
				break
			}
		}
		sum++
	}

	return sum
}

func (s *RabbitEndpoint) doLuaConsume(req *model.RowRequest, rule *global.Rule) error {
	var err error
	var ls []*model.MQRespond
	kvm := rowMap(req, rule, true)
	if req.Action == canal.UpdateAction {
		previous := oldRowMap(req, rule, true)
		ls, err = luaengine.DoMQOps(kvm, previous, req.Action, rule)
	} else {
		ls, err = luaengine.DoMQOps(kvm, nil, req.Action, rule)
	}
	if err != nil {
		log.Println("Lua 脚本执行失败!!! ,详情请参见日志")
		return errors.Errorf("lua 脚本执行失败 : %s ", err)
	}

	for _, resp := range ls {
		s.mergeQueue(resp.Topic)
		err := s.rabChl.Publish("", resp.Topic, false, false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        resp.ByteArray,
			})

		logs.Infof("topic: %s, message: %s", resp.Topic, string(resp.ByteArray))
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *RabbitEndpoint) doRuleConsume(req *model.RowRequest, rule *global.Rule) error {
	kvm := rowMap(req, rule, false)

	resp := new(model.MQRespond)
	resp.Action = req.Action
	resp.Timestamp = req.Timestamp
	if rule.ValueEncoder == global.ValEncoderJson {
		resp.Date = kvm
	} else {
		resp.Date = encodeValue(rule, kvm)
	}

	if rule.ReserveRawData && canal.UpdateAction == req.Action {
		resp.Raw = oldRowMap(req, rule, false)
	}

	body, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	err = s.rabChl.Publish("", rule.RabbitmqQueue, false, false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
		})

	logs.Infof("topic: %s, message: %s", rule.RabbitmqQueue, string(body))

	return err
}

func (s *RabbitEndpoint) Close() {
	if s.rabChl != nil {
		s.rabChl.Close()
	}
	if s.rabCon != nil {
		s.rabCon.Close()
	}
}

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
	"github.com/siddontang/go-mysql/canal"
	"log"
	"strings"
	"sync"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/apache/rocketmq-client-go/v2/rlog"
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"

	"go-mysql-transfer/global"
	"go-mysql-transfer/metrics"
	"go-mysql-transfer/model"
	"go-mysql-transfer/service/luaengine"
	"go-mysql-transfer/util/logagent"
	"go-mysql-transfer/util/logs"
)

const _rocketRetry = 2

type RocketEndpoint struct {
	client    rocketmq.Producer
	retryLock sync.Mutex
}

func newRocketEndpoint() *RocketEndpoint {
	rlog.SetLogger(logagent.NewRocketmqLoggerAgent())
	cfg := global.Cfg()

	options := make([]producer.Option, 0)
	serverList := strings.Split(cfg.RocketmqNameServers, ",")
	options = append(options, producer.WithNameServer(serverList))
	options = append(options, producer.WithRetry(_rocketRetry))
	if cfg.RocketmqGroupName != "" {
		options = append(options, producer.WithGroupName(cfg.RocketmqGroupName))
	}
	if cfg.RocketmqInstanceName != "" {
		options = append(options, producer.WithInstanceName(cfg.RocketmqInstanceName))
	}
	if cfg.RocketmqAccessKey != "" && cfg.RocketmqSecretKey != "" {
		options = append(options, producer.WithCredentials(primitive.Credentials{
			AccessKey: cfg.RocketmqAccessKey,
			SecretKey: cfg.RocketmqSecretKey,
		}))
	}

	producer, _ := rocketmq.NewProducer(options...)
	r := &RocketEndpoint{}
	r.client = producer
	return r
}

func (s *RocketEndpoint) Connect() error {
	return s.client.Start()
}

func (s *RocketEndpoint) Ping() error {
	ping := &primitive.Message{
		Topic: "BenchmarkTest",
		Body:  []byte("ping"),
	}
	_, err := s.client.SendSync(context.Background(), ping)
	return err
}

func (s *RocketEndpoint) Consume(from mysql.Position, rows []*model.RowRequest) error {
	var ms []*primitive.Message
	for _, row := range rows {
		rule, _ := global.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		metrics.UpdateActionNum(row.Action, row.RuleKey)

		if rule.LuaEnable() {
			ls, err := s.buildMessages(row, rule)
			if err != nil {
				log.Println("Lua 脚本执行失败!!! ,详情请参见日志")
				return errors.Errorf("lua 脚本执行失败 : %s ", errors.ErrorStack(err))
			}
			ms = append(ms, ls...)
		} else {
			m, err := s.buildMessage(row, rule)
			if err != nil {
				return errors.New(errors.ErrorStack(err))
			}
			ms = append(ms, m)
		}
	}

	if len(ms) ==0{
		return nil
	}

	var wg sync.WaitGroup
	wg.Add(1)
	var callbackErr error
	err := s.client.SendAsync(context.Background(),
		func(ctx context.Context, result *primitive.SendResult, e error) {
			if e != nil {
				callbackErr = e
			}
			wg.Done()
		}, ms...)

	if err != nil {
		return err
	}
	wg.Wait()

	if callbackErr != nil {
		return err
	}

	logs.Infof("处理完成 %d 条数据", len(rows))
	return nil
}

func (s *RocketEndpoint) Stock(rows []*model.RowRequest) int64 {
	expect := true
	var ms []*primitive.Message
	for _, row := range rows {
		rule, _ := global.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		if rule.LuaEnable() {
			ls, err := s.buildMessages(row, rule)
			if err != nil {
				logs.Errorf(errors.ErrorStack(err))
				expect = false
				break
			}
			ms = append(ms, ls...)
		} else {
			m, err := s.buildMessage(row, rule)
			if err != nil {
				logs.Errorf(errors.ErrorStack(err))
				expect = false
				break
			}
			ms = append(ms, m)
		}
	}

	if !expect {
		return 0
	}

	if len(ms) == 0 {
		return 0
	}

	var wg sync.WaitGroup
	wg.Add(1)
	err := s.client.SendAsync(context.Background(),
		func(ctx context.Context, result *primitive.SendResult, e error) {
			if e != nil {
				logs.Error(errors.ErrorStack(e))
				expect = false
			}
			wg.Done()
		}, ms...)

	if err != nil {
		logs.Error(errors.ErrorStack(err))
		return 0
	}

	wg.Wait()

	if expect {
		return int64(len(ms))
	}
	return 0
}

func (s *RocketEndpoint) buildMessages(req *model.RowRequest, rule *global.Rule) ([]*primitive.Message, error) {
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
		return nil, errors.Errorf("lua 脚本执行失败 : %s ", err)
	}

	var ms []*primitive.Message
	for _, resp := range ls {
		m := &primitive.Message{
			Topic: resp.Topic,
			Body:  resp.ByteArray,
		}
		logs.Infof("topic: %s, message: %s", m.Topic, string(m.Body))
		ms = append(ms, m)
	}

	return ms, nil
}

func (s *RocketEndpoint) buildMessage(req *model.RowRequest, rule *global.Rule) (*primitive.Message, error) {
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
		return nil, err
	}

	m := &primitive.Message{
		Topic: rule.RocketmqTopic,
		Body:  body,
	}

	logs.Infof("topic: %s, message: %s", m.Topic, string(m.Body))

	return m, nil
}

func (s *RocketEndpoint) Close() {
	if s.client != nil {
		s.client.Shutdown()
	}
}

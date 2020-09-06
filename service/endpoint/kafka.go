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
	"strings"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/pquerna/ffjson/ffjson"
	"github.com/vmihailenco/msgpack"

	"go-mysql-transfer/global"
	"go-mysql-transfer/service/luaengine"
	"go-mysql-transfer/storage"
	"go-mysql-transfer/util/logutil"
)

type KafkaEndpoint struct {
	config *global.Config
	cached *storage.BoltRowStorage

	client   sarama.Client
	producer sarama.AsyncProducer
}

func newKafkaEndpoint(c *global.Config) *KafkaEndpoint {
	r := &KafkaEndpoint{}
	r.config = c
	r.cached = &storage.BoltRowStorage{}
	return r
}

func (s *KafkaEndpoint) Start() error {
	cfg := sarama.NewConfig()
	cfg.Producer.Partitioner = sarama.NewRandomPartitioner

	if s.config.KafkaSASLUser != "" && s.config.KafkaSASLPassword != "" {
		cfg.Net.SASL.Enable = true
		cfg.Net.SASL.User = s.config.KafkaSASLUser
		cfg.Net.SASL.Password = s.config.KafkaSASLPassword
	}

	var err error
	var client sarama.Client
	ls := strings.Split(s.config.KafkaAddr, ",")
	client, err = sarama.NewClient(ls, cfg)
	if err != nil {
		return errors.Errorf("unable to create kafka client: %q", err)
	}

	var producer sarama.AsyncProducer
	producer, err = sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		return errors.Errorf("unable to create kafka producer: %q", err)
	}

	s.producer = producer
	s.client = client

	return nil
}

func (s *KafkaEndpoint) Ping() error {
	return nil
}

func (s *KafkaEndpoint) Consume(rows []*global.RowRequest) {
	if err := s.doRetryTask(); err != nil {
		logutil.Error(err.Error())
		pushFailedRows(rows, s.cached)
		return
	}

	expect := true
	var ms []*sarama.ProducerMessage
	for _, row := range rows {
		rule, _ := global.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logutil.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		exportActionNum(row.Action, row.RuleKey)

		if rule.LuaNecessary() {
			ls, err := s.buildMessages(row, rule)
			if err != nil {
				logutil.Errorf(errors.ErrorStack(err))
				expect = false
				break
			}
			ms = append(ms, ls...)
		} else {
			m, err := s.buildMessage(row, rule)
			if err != nil {
				logutil.Errorf(errors.ErrorStack(err))
				expect = false
				break
			}
			ms = append(ms, m)
		}
	}

	if !expect {
		pushFailedRows(rows, s.cached)
		return
	}

	for _, m := range ms {
		s.producer.Input() <- m
		select {
		case err := <-s.producer.Errors():
			logutil.Error(err.Error())
			expect = false
			break
		default:

		}
	}

	if !expect {
		pushFailedRows(rows, s.cached)
	} else {
		logutil.Infof("处理完成 %d 条数据", len(rows))
	}
}

func (s *KafkaEndpoint) Stock(rows []*global.RowRequest) int64 {
	expect := true
	for _, row := range rows {
		rule, _ := global.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logutil.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		if rule.LuaNecessary() {
			ls, err := s.buildMessages(row, rule)
			if err != nil {
				logutil.Errorf(errors.ErrorStack(err))
				expect = false
				break
			}
			for _, m := range ls {
				s.producer.Input() <- m
				select {
				case err := <-s.producer.Errors():
					logutil.Error(err.Error())
					expect = false
					break
				default:
				}
			}
			if !expect {
				break
			}
		} else {
			m, err := s.buildMessage(row, rule)
			if err != nil {
				logutil.Errorf(errors.ErrorStack(err))
				expect = false
				break
			}
			s.producer.Input() <- m
			select {
			case err := <-s.producer.Errors():
				logutil.Error(err.Error())
				expect = false
				break
			default:

			}
		}
	}

	if !expect {
		return 0
	}

	return int64(len(rows))
}

func (s *KafkaEndpoint) buildMessages(row *global.RowRequest, rule *global.Rule) ([]*sarama.ProducerMessage, error) {
	kvm := keyValueMap(row, rule, true)
	ls, err := luaengine.DoMQOps(kvm, row.Action, rule)
	if err != nil {
		return nil, errors.Errorf("lua 脚本执行失败 : %s ", err)
	}

	var ms []*sarama.ProducerMessage
	for _, resp := range ls {
		m := &sarama.ProducerMessage{
			Topic: resp.Topic,
			Value: sarama.ByteEncoder(resp.ByteArray),
		}

		logutil.Infof("topic: %s, message: %s", resp.Topic, string(resp.ByteArray))

		global.MQRespondPool.Put(resp)
		ms = append(ms, m)
	}

	return ms, nil
}

func (s *KafkaEndpoint) buildMessage(row *global.RowRequest, rule *global.Rule) (*sarama.ProducerMessage, error) {
	kvm := keyValueMap(row, rule, false)
	resp := global.MQRespondPool.Get().(*global.MQRespond)
	resp.Action = row.Action
	if rule.ValueEncoder == global.ValEncoderJson {
		resp.Date = kvm
	} else {
		resp.Date = encodeStringValue(rule, kvm)
	}
	body, err := ffjson.Marshal(resp)
	global.MQRespondPool.Put(resp)
	if err != nil {
		return nil, err
	}
	m := &sarama.ProducerMessage{
		Topic: rule.KafkaTopic,
		Value: sarama.ByteEncoder(body),
	}

	logutil.Infof("topic: %s, message: %s", rule.KafkaTopic, string(body))

	return m, nil
}

func (s *KafkaEndpoint) doRetryTask() error {
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
		err = msgpack.Unmarshal(data, row)
		if err != nil {
			logutil.Errorf(err.Error())
			s.cached.Delete(id)
			continue
		}

		rule, _ := global.RuleIns(row.RuleKey)
		if rule.LuaNecessary() {
			ls, err := s.buildMessages(&row, rule)
			if err != nil {
				return err
			}
			for _, msg := range ls {
				s.producer.Input() <- msg
				select {
				case err := <-s.producer.Errors():
					logutil.Error(err.Error())
					return err
				default:

				}
			}
		} else {
			msg, err := s.buildMessage(&row, rule)
			if err != nil {
				return err
			}
			s.producer.Input() <- msg
			select {
			case err := <-s.producer.Errors():
				logutil.Error(err.Error())
				return err
			default:

			}
		}

		logutil.Infof("cached id :%d , 数据重试成功", id)
		s.cached.Delete(id)
	}

	return nil
}

func (s *KafkaEndpoint) Close() {
	if s.producer != nil {
		s.producer.Close()
	}
	if s.client != nil {
		s.client.Close()
	}
}

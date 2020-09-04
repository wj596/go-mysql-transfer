package endpoint

import (
	"github.com/juju/errors"
	"github.com/pquerna/ffjson/ffjson"
	"github.com/streadway/amqp"
	"github.com/vmihailenco/msgpack"

	"go-mysql-transfer/global"
	"go-mysql-transfer/service/luaengine"
	"go-mysql-transfer/storage"
	"go-mysql-transfer/util/logutil"
)

type RabbitEndpoint struct {
	config *global.Config
	cached *storage.BoltRowStorage

	rabCon *amqp.Connection
	rabChl *amqp.Channel
	queues map[string]bool
}

func newRabbitEndpoint(c *global.Config) *RabbitEndpoint {
	r := &RabbitEndpoint{}
	r.config = c
	r.cached = &storage.BoltRowStorage{}
	r.queues = make(map[string]bool)
	return r
}

func (s *RabbitEndpoint) Start() error {
	con, err := amqp.Dial(s.config.RabbitmqAddr)
	if err != nil {
		return err
	}

	var chl *amqp.Channel
	chl, err = con.Channel()
	if err != nil {
		return err
	}

	s.rabCon = con
	s.rabChl = chl

	for _, rule := range global.RuleInsList() {
		_, err := s.rabChl.QueueDeclare(
			rule.RabbitmqQueue, false, false, false, false, nil,
		)
		if err != nil {
			return err
		}
		s.queues[rule.RabbitmqQueue] = true
	}

	return nil
}

func (s *RabbitEndpoint) Ping() error {
	return nil
}

func (s *RabbitEndpoint) mergeQueue(name string) {
	_, ok := s.queues[name]
	if ok {
		return
	}

	s.rabChl.QueueDeclare(name, false, false, false, false, nil)
	s.queues[name] = true
}

func (s *RabbitEndpoint) Consume(rows []*global.RowRequest) {
	if err := s.doRetryTask(); err != nil {
		logutil.Error(err.Error())
		pushFailedRows(rows, s.cached)
		return
	}

	expect := true
	for _, row := range rows {
		rule, _ := global.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logutil.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		exportActionNum(row.Action, row.RuleKey)

		if rule.LuaNecessary() {
			err := s.doLuaConsume(row, rule)
			if err != nil {
				logutil.Errorf(errors.ErrorStack(err))
				expect = false
				break
			}
		} else {
			err := s.doRuleConsume(row, rule)
			if err != nil {
				logutil.Errorf(errors.ErrorStack(err))
				expect = false
				break
			}
		}
	}

	if !expect {
		pushFailedRows(rows, s.cached)
	} else {
		logutil.Infof("处理完成 %d 条数据", len(rows))
	}
}

func (s *RabbitEndpoint) Stock(rows []*global.RowRequest) int64 {
	var sum int64
	for _, row := range rows {
		rule, _ := global.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logutil.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		if rule.LuaNecessary() {
			err := s.doLuaConsume(row, rule)
			if err != nil {
				logutil.Errorf(errors.ErrorStack(err))
				break
			}
		} else {
			err := s.doRuleConsume(row, rule)
			if err != nil {
				logutil.Errorf(errors.ErrorStack(err))
				break
			}
		}
		sum++
	}

	return sum
}

func (s *RabbitEndpoint) doLuaConsume(row *global.RowRequest, rule *global.Rule) error {
	kvm := keyValueMap(row, rule, true)
	ls, err := luaengine.DoMQOps(kvm, row.Action, rule)
	if err != nil {
		return errors.Errorf("lua 脚本执行失败 : %s ", err)
	}

	for _, resp := range ls {
		s.mergeQueue(resp.Topic)
		err := s.rabChl.Publish("", resp.Topic, false, false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        resp.ByteArray,
			})

		logutil.Infof("topic: %s, message: %s", resp.Topic, string(resp.ByteArray))

		global.MQRespondPool.Put(resp)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *RabbitEndpoint) doRuleConsume(row *global.RowRequest, rule *global.Rule) error {
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
		return err
	}
	err = s.rabChl.Publish("", rule.RabbitmqQueue, false, false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
		})

	logutil.Infof("topic: %s, message: %s", rule.RabbitmqQueue, string(body))

	return err
}

func (s *RabbitEndpoint) doRetryTask() error {
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
			err := s.doLuaConsume(&row, rule)
			if err != nil {
				return err
			}

		} else {
			err := s.doRuleConsume(&row, rule)
			if err != nil {
				return err
			}
		}

		logutil.Infof("cached id :%d , 数据重试成功", id)
		s.cached.Delete(id)
	}

	return nil
}

func (s *RabbitEndpoint) Close() {
	if s.rabChl != nil {
		s.rabChl.Close()
	}
	if s.rabCon != nil {
		s.rabCon.Close()
	}
}

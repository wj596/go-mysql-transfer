package endpoint

import (
	"context"
	"strings"
	"sync"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/apache/rocketmq-client-go/v2/rlog"
	"github.com/juju/errors"
	"github.com/pquerna/ffjson/ffjson"
	"github.com/vmihailenco/msgpack"

	"go-mysql-transfer/global"
	"go-mysql-transfer/service/luaengine"
	"go-mysql-transfer/storage"
	"go-mysql-transfer/util/logutil"
)

const _rocketRetry = 2

type RocketEndpoint struct {
	config *global.Config
	cached *storage.BoltRowStorage

	client rocketmq.Producer
}

func newRocketEndpoint(c *global.Config) *RocketEndpoint {
	rlog.SetLogger(logutil.NewRocketmqLoggerAgent())

	options := make([]producer.Option, 0)
	serverList := strings.Split(c.RocketmqNameServers, ",")
	options = append(options, producer.WithNameServer(serverList))
	options = append(options, producer.WithRetry(_rocketRetry))
	if c.RocketmqGroupName != "" {
		options = append(options, producer.WithGroupName(c.RocketmqGroupName))
	}
	if c.RocketmqInstanceName != "" {
		options = append(options, producer.WithInstanceName(c.RocketmqInstanceName))
	}
	if c.RocketmqAccessKey != "" && c.RocketmqSecretKey != "" {
		options = append(options, producer.WithCredentials(primitive.Credentials{
			AccessKey: c.RocketmqAccessKey,
			SecretKey: c.RocketmqSecretKey,
		}))
	}

	producer, _ := rocketmq.NewProducer(options...)
	r := &RocketEndpoint{}
	r.config = c
	r.client = producer
	r.cached = &storage.BoltRowStorage{}
	return r
}

func (s *RocketEndpoint) Start() error {
	return s.client.Start()
}

func (s *RocketEndpoint) Ping() error {
	return nil
}

func (s *RocketEndpoint) Consume(rows []*global.RowRequest) {
	if err := s.doRetryTask(); err != nil {
		logutil.Error(err.Error())
		pushFailedRows(rows, s.cached)
		return
	}

	expect := true
	var ms []*primitive.Message
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

	var wg sync.WaitGroup
	wg.Add(1)
	err := s.client.SendAsync(context.Background(),
		func(ctx context.Context, result *primitive.SendResult, e error) {
			if e != nil {
				logutil.Error(e.Error())
				expect = false
			}
			wg.Done()
		}, ms...)

	if err != nil {
		logutil.Error(err.Error())
		expect = false
	} else {
		wg.Wait()
	}

	if !expect {
		pushFailedRows(rows, s.cached)
	} else {
		logutil.Infof("处理完成 %d 条数据", len(rows))
	}
}

func (s *RocketEndpoint) Stock(rows []*global.RowRequest) int64 {
	expect := true
	var ms []*primitive.Message
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
				logutil.Error(errors.ErrorStack(e))
				expect = false
			}
			wg.Done()
		}, ms...)

	if err != nil {
		logutil.Error(errors.ErrorStack(err))
		return 0
	}

	wg.Wait()

	if expect {
		return int64(len(ms))
	}
	return 0
}

func (s *RocketEndpoint) buildMessages(row *global.RowRequest, rule *global.Rule) ([]*primitive.Message, error) {
	kvm := keyValueMap(row, rule, true)
	ls, err := luaengine.DoMQOps(kvm, row.Action, rule)
	if err != nil {
		return nil, errors.Errorf("lua 脚本执行失败 : %s ", err)
	}

	var ms []*primitive.Message
	for _, resp := range ls {
		m := &primitive.Message{
			Topic: resp.Topic,
			Body:  resp.ByteArray,
		}
		global.MQRespondPool.Put(resp)

		logutil.Infof("topic: %s, message: %s", m.Topic, string(m.Body))

		ms = append(ms, m)
	}

	return ms, nil
}

func (s *RocketEndpoint) buildMessage(row *global.RowRequest, rule *global.Rule) (*primitive.Message, error) {
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
	m := &primitive.Message{
		Topic: rule.RocketmqTopic,
		Body:  body,
	}

	logutil.Infof("topic: %s, message: %s", m.Topic, string(m.Body))

	return m, nil
}

func (s *RocketEndpoint) doRetryTask() error {
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
				_, err = s.client.SendSync(context.Background(), msg)
				if err != nil {
					return err
				}
				logutil.Infof("retry: topic:%s, message:%s", msg.Topic, string(msg.Body))
			}
		} else {
			msg, err := s.buildMessage(&row, rule)
			if err != nil {
				return err
			}
			_, err = s.client.SendSync(context.Background(), msg)
			if err != nil {
				return err
			}
			logutil.Infof("retry: topic:%s, message:%s", msg.Topic, string(msg.Body))
		}

		logutil.Infof("cached id :%d , 数据重试成功", id)
		s.cached.Delete(id)
	}

	return nil
}

func (s *RocketEndpoint) Close() {
	if s.client != nil {
		s.client.Shutdown()
	}
}

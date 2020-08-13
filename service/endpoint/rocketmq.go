package endpoint

import (
	"context"
	"go.uber.org/atomic"
	"strings"
	"sync"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/apache/rocketmq-client-go/v2/rlog"
	"github.com/vmihailenco/msgpack"

	"go-mysql-transfer/global"
	"go-mysql-transfer/service/luaengine"
	"go-mysql-transfer/util/logutil"
)

const _rocketmqRetry = 2

type RocketmqEndpoint struct {
	client rocketmq.Producer
}

func newRocketmqEndpoint(c *global.Config) *RocketmqEndpoint {
	rlog.SetLogger(logutil.NewRocketmqLoggerAgent())

	options := make([]producer.Option, 0)
	serverList := strings.Split(c.RocketmqNameServers, ",")
	options = append(options, producer.WithNameServer(serverList))
	options = append(options, producer.WithRetry(_rocketmqRetry))
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
	r := &RocketmqEndpoint{}
	r.client = producer

	return r
}

func (s *RocketmqEndpoint) Start() error {
	return s.client.Start()
}

func (s *RocketmqEndpoint) Ping() error {
	return nil
}

func (s *RocketmqEndpoint) Consume(rows []*global.RowRequest) {
	var defeated atomic.Bool
	var wg sync.WaitGroup

	ms, err := s.doConsume(rows)
	if err != nil {
		logutil.Error(err.Error())
		defeated.Store(true)
	} else {
		wg.Add(1)
		err = s.client.SendAsync(context.Background(),
			func(ctx context.Context, result *primitive.SendResult, e error) {
				if e != nil {
					logutil.Error(e.Error())
					defeated.Store(true)
				}
				wg.Done()
			}, ms...)
		if err != nil {
			defeated.Store(true)
			logutil.Error(err.Error())
		} else {
			wg.Wait()
		}
	}

	if defeated.Load() {
		logutil.Infof("%d 条数据处理失败，插入重试队列", len(rows))
		saveFailedRows(rows)
	} else {
		logutil.Infof("处理完成 %d 条数据", len(rows))
	}
}

func (s *RocketmqEndpoint) Stock(rows []*global.RowRequest) int {
	var defeated atomic.Bool
	var wg sync.WaitGroup

	ms, err := s.doConsume(rows)
	msLen := len(ms)
	if err != nil {
		logutil.Error(err.Error())
		defeated.Store(true)
	} else {
		wg.Add(1)
		err = s.client.SendAsync(context.Background(),
			func(ctx context.Context, result *primitive.SendResult, e error) {
				if e != nil {
					logutil.Error(e.Error())
					defeated.Store(true)
				}
				wg.Done()
			}, ms...)
		if err != nil {
			defeated.Store(true)
			logutil.Error(err.Error())
		} else {
			wg.Wait()
		}
	}

	if !defeated.Load() {
		return msLen
	}

	return 0
}

func (s *RocketmqEndpoint) doConsume(rows []*global.RowRequest) ([]*primitive.Message, error) {
	var ms []*primitive.Message
	for _, row := range rows {
		rule, ignore := ignoreRow(row.RuleKey, len(row.Row))
		if ignore {
			continue
		}

		global.IncInsertNum(row.RuleKey)

		kvm := keyValueMap(row, rule)
		if rule.LuaNecessary() {
			responds, err := luaengine.DoRocketmqOps(kvm, rule)
			if err != nil {
				logutil.Error(err.Error())
				return nil, err
			}
			for _, respond := range responds {
				m := &primitive.Message{
					Topic: respond.Topic,
					Body:  respond.Msg,
				}

				global.RocketmqRespondPool.Put(respond)

				ms = append(ms, m)
			}
		} else {
			m := &primitive.Message{
				Topic: rule.RocketmqTopic,
				Body:  encodeByteArrayValue(rule.ValueEncoder, kvm),
			}

			ms = append(ms, m)
		}
	}

	return ms, nil
}

func (s *RocketmqEndpoint) StartRetryTask() {
	ticker := time.NewTicker(_retryInterval * time.Second)
	go func() {
		for {
			<-ticker.C
			if _rowCache.Size() == 0 {
				continue
			}

			logutil.Infof("重试队列有 %d条数据", _rowCache.Size())
			ids, err := _rowCache.IdList()
			if err != nil {
				logutil.Errorf(err.Error())
				continue
			}

			var data []byte
			for _, id := range ids {
				var err error
				data, err = _rowCache.Get(id)
				if err != nil {
					logutil.Warn(err.Error())
					_rowCache.Delete(id)
					continue
				}

				var cached global.RowRequest
				err = msgpack.Unmarshal(data, &cached)
				if err != nil {
					logutil.Errorf(err.Error())
					_rowCache.Delete(data)
					continue
				}

				err = s.doRetry(&cached)
				if err != nil {
					break
				}

				logutil.Infof("数据重试成功,还有%d 条数据等待重试", _rowCache.Size())
				_rowCache.Delete(data)
			}
		}
	}()
}

func (s *RocketmqEndpoint) doRetry(row *global.RowRequest) error {
	rule, ignore := ignoreRow(row.RuleKey, len(row.Row))
	if ignore {
		return nil
	}

	kvm := keyValueMap(row, rule)
	if rule.LuaNecessary() {
		responds, err := luaengine.DoRocketmqOps(kvm, rule)
		if err != nil {
			logutil.Warn(err.Error())
			return nil
		}
		for _, respond := range responds {
			msg := &primitive.Message{
				Topic: respond.Topic,
				Body:  respond.Msg,
			}

			global.RocketmqRespondPool.Put(respond)

			_, err = s.client.SendSync(context.Background(), msg)
			if err != nil {
				logutil.Error(err.Error())
				return err
			}
		}
	} else {
		_, err := s.client.SendSync(context.Background(), &primitive.Message{
			Topic: rule.RocketmqTopic,
			Body:  encodeByteArrayValue(rule.ValueEncoder, kvm),
		})
		if err != nil {
			logutil.Error(err.Error())
			return err
		}
	}

	return nil
}

func (s *RocketmqEndpoint) Close() {
	if s.client != nil {
		s.client.Shutdown()
	}
}

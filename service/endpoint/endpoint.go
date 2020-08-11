package endpoint

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/schema"
	"github.com/vmihailenco/msgpack"

	"go-mysql-transfer/global"
	"go-mysql-transfer/storage"
	"go-mysql-transfer/util/logutil"
)

const _retryInterval = 30

var _rowCache *storage.BoltRowStorage

type Service interface {
	Ping() error
	Consume([]*global.RowRequest)
	Stock([]*global.RowRequest) int
	StartRetryTask()
	Close()
}

func NewEndpointService(c *global.Config) Service {
	_rowCache = &storage.BoltRowStorage{}

	if c.IsRedis() {
		return newRedisEndpoint(c)
	}

	if c.IsRocketmq() {
		return newRocketmqEndpoint(c)
	}

	return nil
}

func convertColumnData(value interface{}, col *schema.TableColumn, rule *global.Rule) interface{} {
	switch col.Type {
	case schema.TYPE_ENUM:
		switch value := value.(type) {
		case int64:
			// for binlog, ENUM may be int64, but for dump, enum is string
			eNum := value - 1
			if eNum < 0 || eNum >= int64(len(col.EnumValues)) {
				// we insert invalid enum value before, so return empty
				logutil.Warnf("invalid binlog enum index %d, for enum %v", eNum, col.EnumValues)
				return ""
			}

			return col.EnumValues[eNum]
		}
	case schema.TYPE_SET:
		switch value := value.(type) {
		case int64:
			// for binlog, SET may be int64, but for dump, SET is string
			bitmask := value
			sets := make([]string, 0, len(col.SetValues))
			for i, s := range col.SetValues {
				if bitmask&int64(1<<uint(i)) > 0 {
					sets = append(sets, s)
				}
			}
			return strings.Join(sets, ",")
		}
	case schema.TYPE_BIT:
		switch value := value.(type) {
		case string:
			// for binlog, BIT is int64, but for dump, BIT is string
			// for dump 0x01 is for 1, \0 is for 0
			if value == "\x01" {
				return int64(1)
			}

			return int64(0)
		}
	case schema.TYPE_STRING:
		switch value := value.(type) {
		case []byte:
			return string(value[:])
		}
	case schema.TYPE_JSON:
		var f interface{}
		var err error
		switch v := value.(type) {
		case string:
			err = json.Unmarshal([]byte(v), &f)
		case []byte:
			err = json.Unmarshal(v, &f)
		}
		if err == nil && f != nil {
			return f
		}
	case schema.TYPE_DATETIME, schema.TYPE_TIMESTAMP:
		switch v := value.(type) {
		case string:
			vt, err := time.ParseInLocation(mysql.TimeFormat, string(v), time.Local)
			if err != nil || vt.IsZero() { // failed to parse date or zero date
				return nil
			}
			return vt.Format(rule.DatetimeFormatter)
		case []byte:
			return string(v)
		}
	case schema.TYPE_DATE:
		switch v := value.(type) {
		case string:
			vt, err := time.Parse(rule.DateFormatter, string(v))
			if err != nil || vt.IsZero() { // failed to parse date or zero date
				return nil
			}
			return vt.Format(rule.DateFormatter)
		case []byte:
			return string(v)
		}
	}

	return value
}

func encodeStringValue(encode string, kv map[string]interface{}) string {
	var val string
	switch encode {
	case global.ValEncoderJson:
		data, _ := json.Marshal(kv)
		val = string(data)
	}

	return val
}

func encodeByteArrayValue(encode string, kv map[string]interface{}) []byte {
	switch encode {
	case global.ValEncoderJson:
		data, _ := json.Marshal(kv)
		return data
	}

	return nil
}

func keyValueMap(re *global.RowRequest, rule *global.Rule) map[string]interface{} {
	kv := make(map[string]interface{}, len(rule.PaddingMap))
	if rule.LuaNecessary() {
		for _, padding := range rule.PaddingMap {
			kv[padding.ColumnName] = convertColumnData(re.Row[padding.ColumnIndex], padding.Column, rule)
		}
		return kv
	}

	for _, padding := range rule.PaddingMap {
		kv[padding.WrapName] = convertColumnData(re.Row[padding.ColumnIndex], padding.Column, rule)
	}
	if rule.HasDefaultVal {
		for k, v := range rule.DefColumnValMap {
			kv[rule.WrapName(k)] = v
		}
	}

	return kv
}

func ignoreRow(ruleKey string, rowLen int) (*global.Rule, bool) {
	rule, ok := global.RuleIns(ruleKey)
	if !ok {
		logutil.Warnf("%s rule is empty", ruleKey)
		return nil, true
	}

	if rowLen != len(rule.TableInfo.Columns) {
		logutil.Warnf("%s schema mismatching", ruleKey)
		return nil, true
	}

	return rule, false
}

func saveFailedRows(rs []*global.RowRequest) {
	list := make([][]byte, 0, len(rs))
	for _, r := range rs {
		if data, err := msgpack.Marshal(r); err == nil {
			list = append(list, data)
		}
	}
	_rowCache.BatchAdd(list)
}

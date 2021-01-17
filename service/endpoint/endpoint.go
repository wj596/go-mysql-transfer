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
	"bytes"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/schema"

	"go-mysql-transfer/global"
	"go-mysql-transfer/model"
	"go-mysql-transfer/service/luaengine"
	"go-mysql-transfer/util/logs"
	"go-mysql-transfer/util/stringutil"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

const defaultDateFormatter = "2006-01-02"

type Endpoint interface {
	Connect() error
	Ping() error
	Consume(mysql.Position, []*model.RowRequest) error
	Stock([]*model.RowRequest) int64
	Close()
}

func NewEndpoint(ds *canal.Canal) Endpoint {
	cfg := global.Cfg()
	luaengine.InitActuator(ds)

	if cfg.IsRedis() {
		return newRedisEndpoint()
	}

	if cfg.IsMongodb() {
		return newMongoEndpoint()
	}

	if cfg.IsRocketmq() {
		return newRocketEndpoint()
	}

	if cfg.IsRabbitmq() {
		return newRabbitEndpoint()
	}

	if cfg.IsKafka() {
		return newKafkaEndpoint()
	}

	if cfg.IsEls() {
		if cfg.ElsVersion == 6 {
			return newElastic6Endpoint()
		}
		if cfg.ElsVersion == 7 {
			return newElastic7Endpoint()
		}
	}

	if cfg.IsScript() {
		return newScriptEndpoint()
	}

	return nil
}

func convertColumnData(value interface{}, col *schema.TableColumn, rule *global.Rule) interface{} {
	if value == nil {
		return nil
	}

	switch col.Type {
	case schema.TYPE_ENUM:
		switch value := value.(type) {
		case int64:
			eNum := value - 1
			if eNum < 0 || eNum >= int64(len(col.EnumValues)) {
				// we insert invalid enum value before, so return empty
				logs.Warnf("invalid binlog enum index %d, for enum %v", eNum, col.EnumValues)
				return ""
			}
			return col.EnumValues[eNum]
		case string:
			return value
		case []byte:
			return string(value)
		}
	case schema.TYPE_SET:
		switch value := value.(type) {
		case int64:
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
		var vv string
		switch v := value.(type) {
		case string:
			vv = v
		case []byte:
			vv = string(v)
		}
		if rule.DatetimeFormatter != "" {
			vt, err := time.Parse(mysql.TimeFormat, vv)
			if err != nil || vt.IsZero() { // failed to parse date or zero date
				return nil
			}
			return vt.Format(rule.DatetimeFormatter)
		}
		return vv
	case schema.TYPE_DATE:
		var vv string
		switch v := value.(type) {
		case string:
			vv = v
		case []byte:
			vv = string(v)
		}
		if rule.DateFormatter != "" {
			vt, err := time.Parse(defaultDateFormatter, vv)
			if err != nil || vt.IsZero() { // failed to parse date or zero date
				return nil
			}
			return vt.Format(rule.DateFormatter)
		}
		return vv
	case schema.TYPE_NUMBER:
		switch v := value.(type) {
		case string:
			vv, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				logs.Error(err.Error())
				return nil
			}
			return vv
		case []byte:
			str := string(v)
			vv, err := strconv.ParseInt(str, 10, 64)
			if err != nil {
				logs.Error(err.Error())
				return nil
			}
			return vv
		}
	case schema.TYPE_DECIMAL, schema.TYPE_FLOAT:
		switch v := value.(type) {
		case string:
			vv, err := strconv.ParseFloat(v, 64)
			if err != nil {
				logs.Error(err.Error())
				return nil
			}
			return vv
		case []byte:
			str := string(v)
			vv, err := strconv.ParseFloat(str, 64)
			if err != nil {
				logs.Error(err.Error())
				return nil
			}
			return vv
		}
	}

	return value
}

func encodeValue(rule *global.Rule, kv map[string]interface{}) string {
	if rule.ValueTmpl != nil {
		var tmplBytes bytes.Buffer
		err := rule.ValueTmpl.Execute(&tmplBytes, kv)
		if err != nil {
			return ""
		}
		return tmplBytes.String()
	}

	var val string
	switch rule.ValueEncoder {
	case global.ValEncoderJson:
		data, err := json.Marshal(kv)
		if err != nil {
			logs.Error(err.Error())
			val = ""
		} else {
			val = string(data)
		}
	case global.ValEncoderKVCommas:
		var ls []string
		for k, v := range kv {
			str := stringutil.ToString(k) + "=" + stringutil.ToString(v)
			ls = append(ls, str)
		}
		val = strings.Join(ls, ",")
	case global.ValEncoderVCommas:
		var ls []string
		for _, v := range kv {
			ls = append(ls, stringutil.ToString(v))
		}
		val = strings.Join(ls, ",")
	}

	return val
}

func rowMap(req *model.RowRequest, rule *global.Rule, primitive bool) map[string]interface{} {
	kv := make(map[string]interface{}, len(rule.PaddingMap))

	if rule.DefaultColumnValueConfig != "" {
		for k, v := range rule.DefaultColumnValueMap {
			if primitive {
				kv[k] = v
			} else {
				kv[rule.WrapName(k)] = v
			}
		}
	}

	if primitive {
		for _, padding := range rule.PaddingMap {
			kv[padding.ColumnName] = convertColumnData(req.Row[padding.ColumnIndex], padding.ColumnMetadata, rule)
		}
	} else {
		for _, padding := range rule.PaddingMap {
			kv[padding.WrapName] = convertColumnData(req.Row[padding.ColumnIndex], padding.ColumnMetadata, rule)
		}
	}
	return kv
}

func oldRowMap(req *model.RowRequest, rule *global.Rule, primitive bool) map[string]interface{} {
	kv := make(map[string]interface{}, len(rule.PaddingMap))

	if rule.DefaultColumnValueConfig != "" {
		for k, v := range rule.DefaultColumnValueMap {
			if primitive {
				kv[k] = v
			} else {
				kv[rule.WrapName(k)] = v
			}
		}
	}

	if primitive {
		for _, padding := range rule.PaddingMap {
			kv[padding.ColumnName] = convertColumnData(req.Old[padding.ColumnIndex], padding.ColumnMetadata, rule)
		}
	} else {
		for _, padding := range rule.PaddingMap {
			kv[padding.WrapName] = convertColumnData(req.Old[padding.ColumnIndex], padding.ColumnMetadata, rule)
		}
	}
	return kv
}

func primaryKey(re *model.RowRequest, rule *global.Rule) interface{} {
	if rule.IsCompositeKey { // 组合ID
		var key string
		for _, index := range rule.TableInfo.PKColumns {
			key += stringutil.ToString(re.Row[index])
		}
		return key
	} else {
		index := rule.TableInfo.PKColumns[0]
		data := re.Row[index]
		column := rule.TableInfo.Columns[index]
		return convertColumnData(data, &column, rule)
	}
}

func elsHosts(addr string) []string {
	var hosts []string
	splits := strings.Split(addr, ",")
	for _, split := range splits {
		if !strings.HasPrefix(split, "http:") {
			hosts = append(hosts, "http://"+split)
		} else {
			hosts = append(hosts, split)
		}
	}

	return hosts
}

func buildPropertiesByRule(rule *global.Rule) map[string]interface{} {
	properties := make(map[string]interface{})
	for _, padding := range rule.PaddingMap {
		property := make(map[string]interface{})
		switch padding.ColumnType {
		case schema.TYPE_BINARY:
			property["type"] = "binary"
		case schema.TYPE_NUMBER:
			property["type"] = "long"
		case schema.TYPE_DECIMAL:
			property["type"] = "double"
		case schema.TYPE_FLOAT:
			property["type"] = "float"
		case schema.TYPE_DATE:
			property["type"] = "date"
			property["format"] = "yyyy-MM-dd"
		case schema.TYPE_DATETIME, schema.TYPE_TIMESTAMP:
			property["type"] = "date"
			property["format"] = "yyyy-MM-dd HH:mm:ss"
		default:
			property["type"] = "keyword"
		}
		properties[padding.WrapName] = property
	}

	if len(rule.DefaultColumnValueMap) > 0 {
		for key, _ := range rule.DefaultColumnValueMap {
			property := make(map[string]interface{})
			property["type"] = "keyword"
			properties[key] = property
		}
	}

	for _, mapping := range rule.EsMappings {
		property := make(map[string]interface{})
		property["type"] = mapping.Type
		if mapping.Format != "" {
			property["format"] = mapping.Format
		}
		if mapping.Analyzer != "" {
			property["analyzer"] = mapping.Analyzer
		}
		properties[mapping.Field] = property
	}

	return properties
}

func buildPropertiesByMappings(rule *global.Rule) map[string]interface{} {
	properties := make(map[string]interface{})
	for _, mapping := range rule.EsMappings {
		property := make(map[string]interface{})
		property["type"] = mapping.Type
		if mapping.Format != "" {
			property["format"] = mapping.Format
		}
		if mapping.Analyzer != "" {
			property["analyzer"] = mapping.Analyzer
		}
		properties[mapping.Field] = property
	}
	return properties
}

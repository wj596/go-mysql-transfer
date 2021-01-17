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
package global

import (
	"io/ioutil"
	"path/filepath"
	"strings"
	"sync"
	"text/template"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/schema"
	"github.com/vmihailenco/msgpack"
	"github.com/yuin/gopher-lua"
	"github.com/yuin/gopher-lua/parse"

	"go-mysql-transfer/model"
	"go-mysql-transfer/util/dates"
	"go-mysql-transfer/util/files"
	"go-mysql-transfer/util/stringutil"
)

const (
	RedisStructureString    = "String"
	RedisStructureHash      = "Hash"
	RedisStructureList      = "List"
	RedisStructureSet       = "Set"
	RedisStructureSortedSet = "SortedSet"

	ValEncoderJson     = "json"
	ValEncoderKVCommas = "kv-commas"
	ValEncoderVCommas  = "v-commas"
)

var (
	_ruleInsMap       = make(map[string]*Rule)
	_lockOfRuleInsMap sync.RWMutex
)

type EsMapping struct {
	Column   string `yaml:"column"`   // 数据库列名称
	Field    string `yaml:"field"`    // 映射后的ES字段名称
	Type     string `yaml:"type"`     // ES字段类型
	Analyzer string `yaml:"analyzer"` // ES分词器
	Format   string `yaml:"format"`   // 日期格式
}

type Rule struct {
	Schema                   string `yaml:"schema"`
	Table                    string `yaml:"table"`
	OrderByColumn            string `yaml:"order_by_column"`
	ColumnLowerCase          bool   `yaml:"column_lower_case"`          // 列名称转为小写
	ColumnUpperCase          bool   `yaml:"column_upper_case"`          // 列名称转为大写
	ColumnUnderscoreToCamel  bool   `yaml:"column_underscore_to_camel"` // 列名称下划线转驼峰
	IncludeColumnConfig      string `yaml:"include_columns"`            // 包含的列
	ExcludeColumnConfig      string `yaml:"exclude_columns"`            // 排除掉的列
	ColumnMappingConfigs     string `yaml:"column_mappings"`            // 列名称映射
	DefaultColumnValueConfig string `yaml:"default_column_values"`      // 默认的字段和值
	// #值编码，支持json、kv-commas、v-commas；默认为json；json形如：{"id":123,"name":"wangjie"} 、kv-commas形如：id=123,name="wangjie"、v-commas形如：123,wangjie
	ValueEncoder      string `yaml:"value_encoder"`
	ValueFormatter    string `yaml:"value_formatter"`    //格式化定义key,{id}表示字段id的值、{name}表示字段name的值
	LuaScript         string `yaml:"lua_script"`         //lua 脚本
	LuaFilePath       string `yaml:"lua_file_path"`      //lua 文件地址
	DateFormatter     string `yaml:"date_formatter"`     //date类型格式化， 不填写默认2006-01-02
	DatetimeFormatter string `yaml:"datetime_formatter"` //datetime、timestamp类型格式化，不填写默认RFC3339(2006-01-02T15:04:05Z07:00)

	ReserveRawData bool `yaml:"reserve_raw_data"` // 保留update之前的数据，针对KAFKA、RABBITMQ、ROCKETMQ有效

	// ------------------- REDIS -----------------
	//对应redis的5种数据类型 String、Hash(字典) 、List(列表) 、Set(集合)、Sorted Set(有序集合)
	RedisStructure string `yaml:"redis_structure"`
	RedisKeyPrefix string `yaml:"redis_key_prefix"` //key的前缀
	RedisKeyColumn string `yaml:"redis_key_column"` //使用哪个列的值作为key，不填写默认使用主键
	// 格式化定义key,如{id}-{name}；{id}表示字段id的值、{name}表示字段name的值
	RedisKeyFormatter string `yaml:"redis_key_formatter"`
	RedisKeyValue     string `yaml:"redis_key_value"` // key的值，固定值
	// hash的field前缀，仅redis_structure为hash时起作用
	RedisHashFieldPrefix string `yaml:"redis_hash_field_prefix"`
	// 使用哪个列的值作为hash的field，仅redis_structure为hash时起作用
	RedisHashFieldColumn string `yaml:"redis_hash_field_column"`
	// Sorted Set(有序集合)的Score
	RedisSortedSetScoreColumn      string `yaml:"redis_sorted_set_score_column"`
	RedisKeyColumnIndex            int
	RedisKeyColumnIndexs           []int
	RedisHashFieldColumnIndex      int
	RedisHashFieldColumnIndexs     []int
	RedisSortedSetScoreColumnIndex int
	RedisKeyTmpl                   *template.Template

	// ------------------- ROCKETMQ -----------------
	RocketmqTopic string `yaml:"rocketmq_topic"` //rocketmq topic名称，可以为空，为空时使用表名称

	// ------------------- MONGODB -----------------
	MongodbDatabase   string `yaml:"mongodb_database"`   //mongodb database 不能为空
	MongodbCollection string `yaml:"mongodb_collection"` //mongodb collection，可以为空，默认使用表(Table)名称

	// ------------------- RABBITMQ -----------------
	RabbitmqQueue string `yaml:"rabbitmq_queue"` //queue名称,可以为空，默认使用表(Table)名称

	// ------------------- KAFKA -----------------
	KafkaTopic string `yaml:"kafka_topic"` //TOPIC名称,可以为空，默认使用表(Table)名称

	// ------------------- ES -----------------
	ElsIndex   string       `yaml:"es_index"`    //Elasticsearch Index,可以为空，默认使用表(Table)名称
	ElsType    string       `yaml:"es_type"`     //es6.x以后一个Index只能拥有一个Type,可以为空，默认使用_doc; es7.x版本此属性无效
	EsMappings []*EsMapping `yaml:"es_mappings"` //Elasticsearch mappings映射关系,可以为空，为空时根据数据类型自己推导

	// --------------- no config ----------------
	TableInfo             *schema.Table
	TableColumnSize       int
	IsCompositeKey        bool //是否联合主键
	DefaultColumnValueMap map[string]string
	PaddingMap            map[string]*model.Padding
	LuaProto              *lua.FunctionProto
	LuaFunction           *lua.LFunction
	ValueTmpl             *template.Template
}

func RuleDeepClone(res *Rule) (*Rule, error) {
	data, err := msgpack.Marshal(res)
	if err != nil {
		return nil, err
	}

	var r Rule
	err = msgpack.Unmarshal(data, &r)
	if err != nil {
		return nil, err
	}

	return &r, nil
}

func RuleKey(schema string, table string) string {
	return strings.ToLower(schema + ":" + table)
}

func AddRuleIns(ruleKey string, r *Rule) {
	_lockOfRuleInsMap.Lock()
	defer _lockOfRuleInsMap.Unlock()

	_ruleInsMap[ruleKey] = r
}

func RuleIns(ruleKey string) (*Rule, bool) {
	_lockOfRuleInsMap.RLock()
	defer _lockOfRuleInsMap.RUnlock()

	r, ok := _ruleInsMap[ruleKey]

	return r, ok
}

func RuleInsExist(ruleKey string) bool {
	_lockOfRuleInsMap.RLock()
	defer _lockOfRuleInsMap.RUnlock()

	_, ok := _ruleInsMap[ruleKey]

	return ok
}

func RuleInsTotal() int {
	_lockOfRuleInsMap.RLock()
	defer _lockOfRuleInsMap.RUnlock()

	return len(_ruleInsMap)
}

func RuleInsList() []*Rule {
	_lockOfRuleInsMap.RLock()
	defer _lockOfRuleInsMap.RUnlock()

	list := make([]*Rule, 0, len(_ruleInsMap))
	for _, rule := range _ruleInsMap {
		list = append(list, rule)
	}

	return list
}

func RuleKeyList() []string {
	_lockOfRuleInsMap.RLock()
	defer _lockOfRuleInsMap.RUnlock()

	list := make([]string, 0, len(_ruleInsMap))
	for k, _ := range _ruleInsMap {
		list = append(list, k)
	}

	return list
}

func (s *Rule) Initialize() error {
	if err := s.buildPaddingMap(); err != nil {
		return err
	}

	if s.ValueEncoder == "" {
		s.ValueEncoder = ValEncoderJson
	}

	if s.ValueFormatter != "" {
		tmpl, err := template.New(s.TableInfo.Name).Parse(s.ValueFormatter)
		if err != nil {
			return err
		}
		s.ValueTmpl = tmpl
		s.ValueEncoder = ""
	}

	if s.DefaultColumnValueConfig != "" {
		dm := make(map[string]string)
		for _, t := range strings.Split(s.DefaultColumnValueConfig, ",") {
			tt := strings.Split(t, "=")
			if len(tt) != 2 {
				return errors.Errorf("default_field_value format error in rule")
			}
			field := tt[0]
			value := tt[1]
			dm[field] = value
		}
		s.DefaultColumnValueMap = dm
	}

	if s.DateFormatter != "" {
		s.DateFormatter = dates.ConvertGoFormat(s.DateFormatter)
	}

	if s.DatetimeFormatter != "" {
		s.DatetimeFormatter = dates.ConvertGoFormat(s.DatetimeFormatter)
	}

	if _config.IsRedis() {
		if err := s.initRedisConfig(); err != nil {
			return err
		}
	}

	if _config.IsRocketmq() {
		if err := s.initRocketConfig(); err != nil {
			return err
		}
	}

	if _config.IsMongodb() {
		if err := s.initMongoConfig(); err != nil {
			return err
		}
	}

	if _config.IsRabbitmq() {
		if err := s.initRabbitmqConfig(); err != nil {
			return err
		}
	}

	if _config.IsKafka() {
		if err := s.initKafkaConfig(); err != nil {
			return err
		}
	}

	if _config.IsEls() {
		if err := s.initElsConfig(); err != nil {
			return err
		}
	}

	if _config.IsScript() {
		if s.LuaScript == "" && s.LuaFilePath == "" {
			return errors.New("empty lua script not allowed")
		}
	}

	return nil
}

func (s *Rule) AfterUpdateTableInfo() error {
	if err := s.buildPaddingMap(); err != nil {
		return err
	}

	if _config.IsRedis() {
		if err := s.initRedisConfig(); err != nil {
			return err
		}
	}

	if _config.IsRocketmq() {
		if err := s.initRocketConfig(); err != nil {
			return err
		}
	}

	if _config.IsMongodb() {
		if err := s.initMongoConfig(); err != nil {
			return err
		}
	}

	if _config.IsRabbitmq() {
		if err := s.initRabbitmqConfig(); err != nil {
			return err
		}
	}

	if _config.IsKafka() {
		if err := s.initKafkaConfig(); err != nil {
			return err
		}
	}

	if _config.IsEls() {
		if err := s.initElsConfig(); err != nil {
			return err
		}
	}

	if _config.IsScript() {
		if s.LuaScript == "" || s.LuaFilePath == "" {
			return errors.New("empty lua script not allowed")
		}
	}

	return nil
}

func (s *Rule) buildPaddingMap() error {
	paddingMap := make(map[string]*model.Padding)
	mappings := make(map[string]string)

	if s.ColumnMappingConfigs != "" {
		ls := strings.Split(s.ColumnMappingConfigs, ",")
		for _, t := range ls {
			cmc := strings.Split(t, "=")
			if len(cmc) != 2 {
				return errors.Errorf("column_mappings format error in rule")
			}
			column := cmc[0]
			mapped := cmc[1]
			_, index := s.TableColumn(column)
			if index < 0 {
				return errors.Errorf("column_mappings must be table column")
			}
			mappings[strings.ToUpper(column)] = mapped
		}
	}

	if len(s.EsMappings) > 0 {
		for _, mapping := range s.EsMappings {
			mappings[strings.ToUpper(mapping.Column)] = mapping.Field
		}
	}

	var includes []string
	var excludes []string

	if s.IncludeColumnConfig != "" {
		includes = strings.Split(s.IncludeColumnConfig, ",")
	}
	if s.ExcludeColumnConfig != "" {
		excludes = strings.Split(s.ExcludeColumnConfig, ",")
	}

	if len(includes) > 0 {
		for _, c := range includes {
			_, index := s.TableColumn(c)
			if index < 0 {
				return errors.New("include_field must be table column")
			}
			paddingMap[c] = s.newPadding(mappings, c)
		}
	} else {
		for _, column := range s.TableInfo.Columns {
			include := true
			for _, exclude := range excludes {
				if column.Name == exclude {
					include = false
				}
			}
			if include {
				paddingMap[column.Name] = s.newPadding(mappings, column.Name)
			}
		}
	}

	s.PaddingMap = paddingMap

	return nil
}

func (s *Rule) newPadding(mappings map[string]string, columnName string) *model.Padding {
	column, index := s.TableColumn(columnName)

	wrapName := s.WrapName(column.Name)
	mapped, exist := mappings[strings.ToUpper(column.Name)]
	if exist {
		wrapName = mapped
	}

	return &model.Padding{
		WrapName: wrapName,

		ColumnIndex:    index,
		ColumnName:     column.Name,
		ColumnType:     column.Type,
		ColumnMetadata: column,
	}
}

func (s *Rule) TableColumn(field string) (*schema.TableColumn, int) {
	for index, c := range s.TableInfo.Columns {
		if strings.ToUpper(c.Name) == strings.ToUpper(field) {
			return &c, index
		}
	}
	return nil, -1
}

func (s *Rule) WrapName(fieldName string) string {
	if s.ColumnUnderscoreToCamel {
		return stringutil.Case2Camel(strings.ToLower(fieldName))
	}
	if s.ColumnLowerCase {
		return strings.ToLower(fieldName)
	}
	if s.ColumnUpperCase {
		return strings.ToUpper(fieldName)
	}
	return fieldName
}

func (s *Rule) LuaEnable() bool {
	if s.LuaScript == "" && s.LuaFilePath == "" {
		return false
	}

	return true
}

func (s *Rule) initRedisConfig() error {
	if s.LuaEnable() {
		return nil
	}

	if s.RedisStructure == "" {
		return errors.Errorf("empty redis_structure not allowed in rule")
	}

	switch strings.ToUpper(s.RedisStructure) {
	case "STRING":
		s.RedisStructure = RedisStructureString
		if s.RedisKeyColumn == "" && s.RedisKeyFormatter == "" {
			if s.IsCompositeKey {
				for _, v := range s.TableInfo.PKColumns {
					s.RedisKeyColumnIndexs = append(s.RedisKeyColumnIndexs, v)
				}
				s.RedisKeyColumnIndex = -1
			} else {
				s.RedisKeyColumnIndex = s.TableInfo.PKColumns[0]
			}
		}
	case "HASH":
		s.RedisStructure = RedisStructureHash
		if s.RedisKeyValue == "" {
			return errors.New("empty redis_key_value not allowed")
		}
		// init hash field
		if s.RedisHashFieldColumn == "" {
			if s.IsCompositeKey {
				for _, v := range s.TableInfo.PKColumns {
					s.RedisHashFieldColumnIndexs = append(s.RedisHashFieldColumnIndexs, v)
				}
				s.RedisHashFieldColumnIndex = -1
			} else {
				s.RedisHashFieldColumnIndex = s.TableInfo.PKColumns[0]
			}
		} else {
			_, index := s.TableColumn(s.RedisHashFieldColumn)
			if index < 0 {
				return errors.New("redis_hash_field_column must be table column")
			}
			s.RedisHashFieldColumnIndex = index
		}
	case "LIST":
		s.RedisStructure = RedisStructureList
		if s.RedisKeyValue == "" {
			return errors.New("empty redis_key_value not allowed in rule")
		}
	case "SET":
		s.RedisStructure = RedisStructureSet
		if s.RedisKeyValue == "" {
			return errors.New("empty redis_key_value not allowed in rule")
		}
	case "SORTEDSET":
		s.RedisStructure = RedisStructureSortedSet
		if s.RedisKeyValue == "" {
			return errors.New("empty redis_key_value not allowed in rule")
		}
		if s.RedisSortedSetScoreColumn == "" {
			return errors.New("empty redis_sorted_set_score_column not allowed in rule")
		}
		_, index := s.TableColumn(s.RedisSortedSetScoreColumn)
		if index < 0 {
			return errors.New("redis_sorted_set_score_column must be table column")
		}
		s.RedisHashFieldColumnIndex = index
	default:
		return errors.Errorf("redis_structure must be string or hash or list or set")
	}

	if s.RedisKeyColumn != "" {
		_, index := s.TableColumn(s.RedisKeyColumn)
		if index < 0 {
			return errors.New("redis_key_column must be table column")
		}
		s.RedisKeyColumnIndex = index
		s.RedisKeyFormatter = ""
	}

	if s.RedisKeyFormatter != "" {
		tmpl, err := template.New(s.TableInfo.Name).Parse(s.RedisKeyFormatter)
		if err != nil {
			return err
		}
		s.RedisKeyTmpl = tmpl
		s.RedisKeyColumnIndex = -1
	}

	return nil
}

func (s *Rule) initRocketConfig() error {
	if !s.LuaEnable() {
		if s.RocketmqTopic == "" {
			s.RocketmqTopic = s.Table
		}
	}

	return nil
}

func (s *Rule) initMongoConfig() error {
	if !s.LuaEnable() {
		if s.MongodbDatabase == "" {
			return errors.New("empty mongodb_database not allowed in rule")
		}

		if s.MongodbCollection == "" {
			s.MongodbCollection = s.Table
		}
	}

	return nil
}

func (s *Rule) initRabbitmqConfig() error {
	if !s.LuaEnable() {
		if s.RabbitmqQueue == "" {
			s.RabbitmqQueue = s.Table
		}
	}

	return nil
}

func (s *Rule) initElsConfig() error {
	if s.ElsIndex == "" {
		s.ElsIndex = s.Table
	}

	if s.ElsType == "" {
		s.ElsType = "_doc"
	}

	if len(s.EsMappings) > 0 {
		for _, m := range s.EsMappings {
			if m.Field == "" {
				return errors.New("empty field not allowed in es_mappings")
			}
			if m.Type == "" {
				return errors.New("empty type not allowed in es_mappings")
			}
			if m.Column == "" && !s.LuaEnable() {
				return errors.New("empty column not allowed in es_mappings")
			}
		}
	}

	return nil
}

func (s *Rule) initKafkaConfig() error {
	if !s.LuaEnable() {
		if s.KafkaTopic == "" {
			s.KafkaTopic = s.Table
		}
	}

	return nil
}

// 编译Lua
func (s *Rule) CompileLuaScript(dataDir string) error {
	script := s.LuaScript
	if s.LuaFilePath != "" {
		var filePath string
		if files.IsExist(s.LuaFilePath) {
			filePath = s.LuaFilePath
		} else {
			filePath = filepath.Join(dataDir, s.LuaFilePath)
		}
		data, err := ioutil.ReadFile(filePath)
		if err != nil {
			return err
		}
		script = string(data)
	}

	if script == "" {
		return errors.New("empty lua script not allowed")
	}

	s.LuaScript = script

	if _config.IsRedis() {
		if !strings.Contains(script, `require("redisOps")`) {
			return errors.New("lua script incorrect format")
		}

		if !(strings.Contains(script, `SET(`) ||
			strings.Contains(script, `HSET(`) ||
			strings.Contains(script, `RPUSH(`) ||
			strings.Contains(script, `SADD(`) ||
			strings.Contains(script, `ZADD(`) ||
			strings.Contains(script, `DEL(`) ||
			strings.Contains(script, `HDEL(`) ||
			strings.Contains(script, `LREM(`) ||
			strings.Contains(script, `ZREM(`) ||
			strings.Contains(script, `SREM(`)) {

			return errors.New("lua script incorrect format")
		}
	}

	if _config.IsRocketmq() {
		if !strings.Contains(script, `require("mqOps")`) {
			return errors.New("lua script incorrect format")
		}

		if !(strings.Contains(script, `SEND(`)) {
			return errors.New("lua script incorrect format")
		}
	}

	if _config.IsEls() {
		if !strings.Contains(script, `require("esOps")`) {
			return errors.New("lua script incorrect format")
		}
	}

	reader := strings.NewReader(script)
	chunk, err := parse.Parse(reader, script)
	if err != nil {
		return err
	}

	var proto *lua.FunctionProto
	proto, err = lua.Compile(chunk, script)
	if err != nil {
		return err
	}

	s.LuaProto = proto

	return nil
}

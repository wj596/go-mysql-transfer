package global

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/schema"
	"github.com/vmihailenco/msgpack"
	lua "github.com/yuin/gopher-lua"
	"github.com/yuin/gopher-lua/parse"

	"go-mysql-transfer/util/dateutil"
	"go-mysql-transfer/util/fileutil"
	"go-mysql-transfer/util/stringutil"
)

const (
	RedisStructureString = "string"
	RedisStructureHash   = "hash"
	RedisStructureList   = "list"
	RedisStructureSet    = "set"

	ValEncoderJson     = "json"
	ValEncoderKVCommas = "kv-commas"
	ValEncoderVCommas  = "v-commas"
	LeftBrace          = "{+{-{"
	RightBrace         = "}-}+}"
)

var (
	_ruleInsMap       = make(map[string]*Rule)
	_lockOfRuleInsMap sync.RWMutex
)

type Rule struct {
	Schema                  string `yaml:"schema"`
	Table                   string `yaml:"table"`
	OrderByColumn           string `yaml:"order_by_column"`
	ColumnLowerCase         bool   `yaml:"column_lower_case"`          // 列名称转为小写
	ColumnUpperCase         bool   `yaml:"column_upper_case"`          // 列名称转为大写
	ColumnUnderscoreToCamel bool   `yaml:"column_underscore_to_camel"` // 列名称下划线转驼峰
	IncludeColumnConf       string `yaml:"include_column"`             // 包含的列
	ExcludeColumnConf       string `yaml:"exclude_column"`             // 排除掉的列
	ColumnMappingConf       string `yaml:"column_mapping"`             // 列名称映射
	DefColumnValueConf      string `yaml:"default_column_value"`       // 默认的字段和值
	// #值编码，支持json、kv-commas、v-commas；默认为json；json形如：{"id":123,"name":"wangjie"} 、kv-commas形如：id=123,name="wangjie"、v-commas形如：123,wangjie
	ValueEncoder      string `yaml:"value_encoder"`
	ValueFormatter    string `yaml:"value_formatter"`    //格式化定义key,{id}表示字段id的值、{name}表示字段name的值
	LuaScript         string `yaml:"lua_script"`         //lua 脚本
	LuaFilePath       string `yaml:"lua_file_path"`      //lua 文件地址
	DateFormatter     string `yaml:"date_formatter"`     //date类型格式化， 不填写默认2006-01-02
	DatetimeFormatter string `yaml:"datetime_formatter"` //datetime、timestamp类型格式化，不填写默认RFC3339(2006-01-02T15:04:05Z07:00)

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

	RedisKeyIndexListLen int
	RedisKeyIndexList    []int
	RedisKeyIndexMap     map[string]int

	RedisTableHashFieldIndexListLen int
	RedisTableHashFieldIndexList    []int

	// ------------------- ROCKETMQ -----------------
	RocketmqTopic string `yaml:"rocketmq_topic"` //rocketmq topic

	// --------------- no config ----------------
	Enable          bool
	CompositeKey    bool //是否联合主键
	TableInfo       *schema.Table
	HasDefaultVal   bool
	DefColumnValMap map[string]string
	IncludeColumnLs []string // 包含的列
	ExcludeColumnLs []string // 排除掉的列
	PaddingMap      map[string]*Padding
	LuaProto        *lua.FunctionProto
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
	return strings.ToLower(fmt.Sprintf("%s:%s", schema, table))
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

func (s *Rule) Initialize() error {
	if err := s.buildPaddingMap(); err != nil {
		return err
	}

	if s.ValueEncoder == "" {
		s.ValueEncoder = ValEncoderJson
	}
	if s.ValueFormatter != "" {
		s.ValueFormatter = s.rewriteValFormat(s.ValueFormatter)
	}

	if s.DefColumnValueConf != "" {
		defFieldValMap := make(map[string]string)
		for _, t := range strings.Split(s.DefColumnValueConf, ",") {
			tt := strings.Split(t, "=")
			if len(tt) != 2 {
				return errors.Errorf("default_field_value format error in rule")
			}
			field := tt[0]
			value := tt[1]
			defFieldValMap[field] = value
		}
		s.HasDefaultVal = true
		s.DefColumnValMap = defFieldValMap
	}

	if s.DateFormatter == "" {
		s.DateFormatter = "2006-01-02"
	} else {
		s.DateFormatter = dateutil.ConvertGoFormat(s.DateFormatter)
	}

	if s.DatetimeFormatter == "" {
		s.DatetimeFormatter = "2006-01-02 15:04:05"
	} else {
		s.DatetimeFormatter = dateutil.ConvertGoFormat(s.DatetimeFormatter)
	}

	if _config.IsRedis() {
		if err := s.initRedisConfig(); err != nil {
			return err
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
		if err := s.initRocketmqConfig(); err != nil {
			return err
		}
	}

	return nil
}

func (s *Rule) buildPaddingMap() error {
	mappingMap := make(map[string]string)
	if s.ColumnMappingConf != "" {
		fieldMappings := strings.Split(s.ColumnMappingConf, ",")
		for _, t := range fieldMappings {
			tt := strings.Split(t, "=")
			if len(tt) != 2 {
				return errors.Errorf("field-mapping format error in rule")
			}
			field := tt[0]
			mapping := tt[1]
			_, index := s.TableColumn(field)
			if index < 0 {
				return errors.Errorf("field-mapping must be table column")
			}
			mappingMap[strings.ToUpper(field)] = mapping
		}
	}

	if s.IncludeColumnConf != "" {
		s.IncludeColumnLs = strings.Split(s.IncludeColumnConf, ",")
	}

	if s.ExcludeColumnConf != "" {
		s.ExcludeColumnLs = strings.Split(s.ExcludeColumnConf, ",")
	}

	paddingMap := make(map[string]*Padding)
	if s.ValueFormatter == "" {
		if len(s.IncludeColumnLs) > 0 {
			for _, f := range s.IncludeColumnLs {
				columnName := f
				column, index := s.TableColumn(columnName)
				if index < 0 {
					return errors.New("include_field must be table column")
				}
				paddingMap[columnName] = s.newPadding(mappingMap, column, columnName, index)
			}
		} else {
			for index, column := range s.TableInfo.Columns {
				columnName := column.Name
				if s.isExcludeField(columnName) {
					continue
				}
				c := s.TableInfo.Columns[index]
				paddingMap[columnName] = s.newPadding(mappingMap, &c, columnName, index)
			}
		}
	} else { // Format
		reg := regexp.MustCompile("\\{[^\\}]+\\}")
		if reg != nil {
			temps := reg.FindAllString(s.ValueFormatter, -1)
			for _, temp := range temps {
				str := strings.ReplaceAll(temp, "{", "")
				str = strings.ReplaceAll(str, "}", "")

				columnName := strings.ToUpper(str)
				column, index := s.TableColumn(columnName)
				if index < 0 {
					return errors.New("val_format must be table column")
				}
				paddingMap[columnName] = s.newPadding(mappingMap, column, columnName, index)
			}
		}
	}

	s.PaddingMap = paddingMap

	return nil
}

func (s *Rule) TableColumn(field string) (*schema.TableColumn, int) {
	for index, c := range s.TableInfo.Columns {
		if strings.ToUpper(c.Name) == strings.ToUpper(field) {
			return &c, index
		}
	}
	return nil, -1
}

func (s *Rule) newPadding(fieldMapping map[string]string, column *schema.TableColumn, columnName string, columnIndex int) *Padding {
	key := columnName
	if m, b := fieldMapping[strings.ToUpper(columnName)]; b {
		key = m
	}
	key = s.WrapName(key)

	return &Padding{
		Column:      column,
		ColumnName:  columnName,
		ColumnIndex: columnIndex,
		WrapName:    key,
	}
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

func (s *Rule) isExcludeField(field string) bool {
	if len(s.ExcludeColumnLs) == 0 {
		return false
	}

	for _, t := range s.ExcludeColumnLs {
		if strings.ToUpper(t) == strings.ToUpper(field) {
			return true
		}
	}

	return false
}

func (s *Rule) LuaNecessary() bool {
	if s.LuaScript == "" && s.LuaFilePath == "" {
		return false
	}

	return true
}

func (s *Rule) enablePrimaryKey() {
	s.RedisKeyIndexList = make([]int, 0, len(s.TableInfo.PKColumns))
	for _, v := range s.TableInfo.PKColumns {
		s.RedisKeyIndexList = append(s.RedisKeyIndexList, v)
	}
	s.RedisKeyIndexListLen = len(s.RedisKeyIndexList)
}

func (s *Rule) initRedisConfig() error {
	switch s.RedisStructure {
	case RedisStructureString:
		if !s.LuaNecessary() {
			if s.RedisKeyColumn == "" && s.RedisKeyFormatter == "" {
				s.enablePrimaryKey()
			}
		}
	case RedisStructureHash:
		if !s.LuaNecessary() {
			if s.RedisKeyValue == "" {
				return errors.New("empty redis_key_value not allowed")
			}

			// init hash field
			s.RedisTableHashFieldIndexList = make([]int, 0, len(s.TableInfo.PKColumns))
			if s.RedisHashFieldColumn == "" {
				for _, v := range s.TableInfo.PKColumns {
					s.RedisTableHashFieldIndexList = append(s.RedisTableHashFieldIndexList, v)
				}
				s.RedisTableHashFieldIndexListLen = len(s.RedisTableHashFieldIndexList)
			} else {
				_, index := s.TableColumn(s.RedisHashFieldColumn)
				if index < 0 {
					return errors.New("redis_hash_field_column must be table column")
				}
				s.RedisTableHashFieldIndexList = append(s.RedisTableHashFieldIndexList, index)
				s.RedisTableHashFieldIndexListLen = 1
			}
		}
	case RedisStructureList:
		if !s.LuaNecessary() {
			if s.RedisKeyValue == "" {
				return errors.New("empty redis_key_value not allowed")
			}
		}
	case RedisStructureSet:
		if !s.LuaNecessary() {
			if s.RedisKeyValue == "" {
				return errors.New("empty redis_key_value not allowed")
			}
		}
	}

	if !s.LuaNecessary() {
		if s.RedisKeyColumn != "" {
			s.RedisKeyIndexList = make([]int, 0, len(s.TableInfo.PKColumns))
			_, index := s.TableColumn(s.RedisKeyColumn)
			if index < 0 {
				return errors.New("redis_key_column must be table column")
			}
			s.RedisKeyIndexList = append(s.RedisKeyIndexList, index)
			s.RedisKeyIndexListLen = 1
			s.RedisKeyFormatter = ""
		}

		if s.RedisKeyFormatter != "" {
			indexMap := make(map[string]int)
			reg := regexp.MustCompile("\\{[^\\}]+\\}")
			if reg != nil {
				temps := reg.FindAllString(s.RedisKeyFormatter, -1)
				for _, temp := range temps {
					columnName := strings.ReplaceAll(temp, "{", "")
					columnName = strings.ReplaceAll(columnName, "}", "")
					_, index := s.TableColumn(columnName)
					if index < 0 {
						return errors.New("redis_key_formatter must be table column")
					}
					indexMap[columnName] = index
				}
			}
			if len(indexMap) == 0 {
				return errors.New("redis_key_formatter error in rule")
			}
			s.RedisKeyIndexMap = indexMap
			s.RedisKeyFormatter = s.rewriteValFormat(s.RedisKeyFormatter)
		}
	}

	return nil
}

func (s *Rule) initRocketmqConfig() error {
	if !s.LuaNecessary() {
		if s.RocketmqTopic == "" {
			return errors.New("empty rocketmq_topic not allowed in rule")
		}
	}

	return nil
}

func (s *Rule) rewriteValFormat(format string) string {
	temp := format
	for _, c := range temp {
		if string(c) == "{" {
			temp += LeftBrace
		} else if string(c) == "}" {
			temp += RightBrace
		} else {
			temp += string(c)
		}
	}

	return temp
}

func (s *Rule) PreCompileLuaScript(dataDir string) error {
	script := s.LuaScript
	if s.LuaFilePath != "" {
		var filePath string
		if fileutil.IsExist(s.LuaFilePath) {
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

	if !strings.Contains(script, "function") {
		return errors.New("lua script incorrect format")
	}

	if !strings.Contains(script, "transfer(") {
		return errors.New("lua script incorrect format")
	}

	if !strings.Contains(script, "transfer(") {
		return errors.New("lua script incorrect format")
	}

	if _config.IsRedis() {
		if !strings.Contains(script, `require("redisOps")`) {
			return errors.New("lua script incorrect format")
		}
		switch s.RedisStructure {
		case RedisStructureString:
			if !strings.Contains(script, "SET(") {
				return errors.New("lua script incorrect format")
			}
		case RedisStructureHash:
			if !strings.Contains(script, "HSET(") {
				return errors.New("lua script incorrect format")
			}
		case RedisStructureList:
			if !strings.Contains(script, "RPUSH(") {
				return errors.New("lua script incorrect format")
			}
		case RedisStructureSet:
			if !strings.Contains(script, "SADD(") {
				return errors.New("lua script incorrect format")
			}
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

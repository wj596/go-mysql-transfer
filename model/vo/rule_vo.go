package vo

import (
	"fmt"
	"go-mysql-transfer/config"
	"go-mysql-transfer/model/po"
	"go-mysql-transfer/util/stringutils"
)

// ColumnMappingItem 列名映射
type ColumnMappingItem struct {
	Column  string `json:"column"`
	Mapping string `json:"mapping"`
}

// AdditionalColumnValueItem 附加列值
type AdditionalColumnValueItem struct {
	Column string `json:"column"`
	Value  string `json:"value"`
}

// EsIndexMappingItem mappings映射关系
type EsIndexMappingItem struct {
	Column     string `json:"column"`
	EsField    string `json:"esField"`
	EsType     string `json:"esType"`
	EsAnalyzer string `json:"esAnalyzer"`
}

// TransformRuleVO '转换规则'值对象
// see TransformRule
type TransformRuleVO struct {
	Id                                 uint64                      `json:"id,string`
	Key                                string                      `json:"key"`
	PipelineInfoId                     uint64                      `json:"pipelineInfoId,string"`
	SourceId                           uint64                      `json:"sourceId,string"`
	EndpointId                         uint64                      `json:"endpointId,string"`
	EndpointType                       int32                       `json:"endpointType"`
	Type                               int32                       `json:"type"`
	TypeName                           string                      `json:"typeName"`
	ReceiveType                        string                      `json:"receiveType"`
	Schema                             string                      `json:"schema"`
	Table                              string                      `json:"table"`
	ColumnNameFormatter                string                      `json:"columnNameFormatter"`
	ExcludeColumnList                  []string                    `json:"excludeColumnList"`
	ColumnMappingGroups                []ColumnMappingItem         `json:"columnMappingGroups"`
	AdditionalColumnValueMappingGroups []AdditionalColumnValueItem `json:"additionalColumnValueMappingGroups"`
	DataEncoder                        string                      `json:"dataEncoder"`
	DataExpression                     string                      `json:"dataExpression"`
	DateFormatter                      string                      `json:"dateFormatter"`
	DatetimeFormatter                  string                      `json:"datetimeFormatter"`
	OrderColumn                        string                      `json:"orderColumn"`

	RedisStructure            string `json:"redisStructure"`
	RedisKeyPrefix            string `json:"redisKeyPrefix"`
	RedisKeyBuilder           string `json:"redisKeyBuilder"`
	RedisKeyColumn            string `json:"redisKeyColumn"`
	RedisKeyExpression        string `json:"redisKeyExpression"`
	RedisKeyFixValue          string `json:"redisKeyFixValue"`
	RedisHashFieldPrefix      string `json:"redisHashFieldPrefix"`
	RedisHashFieldColumn      string `json:"redisHashFieldColumn"`
	RedisSortedSetScoreColumn string `json:"redisSortedSetScoreColumn"`

	EsIndexBuildType     string               `json:"esIndexBuildType"`
	EsIndexName          string               `json:"esIndexName"`
	EsIndexMappingGroups []EsIndexMappingItem `json:"esIndexMappingGroups"`

	MongodbDatabase   string `json:"mongodbDatabase"`
	MongodbCollection string `json:"mongodbCollection"`

	MqTopic          string `json:"mqTopic"`
	MqReserveRawData bool   `json:"mqReserveRawData"`

	HttpParameterName  string `json:"httpParameterName"`
	HttpReserveRawData bool   `json:"httpReserveRawData"`

	LuaScript string `json:"luaScript"`
}

func (s *TransformRuleVO) ToPO() *po.TransformRule {
	if s.Type == config.TransformRuleLuaScript {
		return &po.TransformRule{
			Type:      s.Type, //0规则 1脚本
			Schema:    s.Schema,
			Table:     s.Table,
			LuaScript: s.LuaScript,
		}
	}

	p := &po.TransformRule{
		Type:                s.Type, //0规则 1脚本
		EndpointType:        s.EndpointType,
		ReceiveType:         stringutils.ToInt32Safe(s.ReceiveType),
		Schema:              s.Schema,
		Table:               s.Table,
		ColumnNameFormatter: stringutils.ToInt32Safe(s.ColumnNameFormatter), //列名转换格式 0:列名称转为小写 1:列名称转为大写 2:列名称下划线转驼峰
		ExcludeColumnList:   s.ExcludeColumnList,                            // 排除掉的列
		DataEncoder:         stringutils.ToInt32Safe(s.DataEncoder),         //数据编码类型，0: json、 1:表达式
		DataExpression:      s.DataExpression,                               //数据expression
		DateFormatter:       s.DateFormatter,       //date类型格式化
		DatetimeFormatter:   s.DatetimeFormatter,   //datetime、timestamp类型格式化
		OrderColumn:         s.OrderColumn,
	}

	if nil != s.ColumnMappingGroups && len(s.ColumnMappingGroups) > 0 {
		columnNameMapping := make(map[string]string)
		for _, v := range s.ColumnMappingGroups {
			columnNameMapping[v.Column] = v.Mapping
		}
		p.ColumnNameMapping = columnNameMapping
	}
	if nil != s.AdditionalColumnValueMappingGroups && len(s.AdditionalColumnValueMappingGroups) > 0 {
		additionalColumnValueMapping := make(map[string]string)
		for _, v := range s.AdditionalColumnValueMappingGroups {
			additionalColumnValueMapping[v.Column] = v.Value
		}
		p.AdditionalColumnValueMapping = additionalColumnValueMapping
	}

	if s.EndpointType == config.EndpointTypeRedis {
		p.RedisStructure = stringutils.ToInt32Safe(s.RedisStructure)   //对应redis的5种数据类型 1:String、2:Hash(字典) 、3:List(列表) 、4:Set(集合)、5:Sorted Set(有序集合)
		p.RedisKeyPrefix = s.RedisKeyPrefix                            //key的前缀
		p.RedisKeyBuilder = stringutils.ToInt32Safe(s.RedisKeyBuilder) //key生成方式，0:使用列值(默认使用主键)、1:表达式、2固定值
		p.RedisKeyColumn = s.RedisKeyColumn                            //key生成方式，使用列值，默认使用主键
		p.RedisKeyExpression = s.RedisKeyExpression                    // key生成表达式
		p.RedisKeyFixValue = s.RedisKeyFixValue                        // key固定值
		p.RedisHashFieldPrefix = s.RedisHashFieldPrefix                // hash的field前缀，仅redis_structure为hash时起作用
		p.RedisHashFieldColumn = s.RedisHashFieldColumn                // 使用哪个列的值作为hash的field，仅redis_structure为hash时起作用
		p.RedisSortedSetScoreColumn = s.RedisSortedSetScoreColumn      // Sorted Set(有序集合)的Score
	}

	if s.EndpointType == config.EndpointTypeMongoDB {
		p.MongodbDatabase = s.MongodbDatabase
		p.MongodbCollection = s.MongodbCollection
	}

	if s.EndpointType == config.EndpointTypeElasticsearch {
		p.EsIndexBuildType = stringutils.ToInt32Safe(s.EsIndexBuildType) //Index名称创建方式，0使用已经存在的、1自动创建
		p.EsIndexName = s.EsIndexName
		if nil != s.EsIndexMappingGroups && len(s.EsIndexMappingGroups) > 0 {
			esIndexMappings := make([]*po.EsIndexMapping, len(s.EsIndexMappingGroups))
			for i, v := range s.EsIndexMappingGroups {
				m := &po.EsIndexMapping{
					Column:     v.Column,  // 数据库列名称
					EsField:    v.EsField, // 映射后的ES字段名称
					EsType:     v.EsType,  // ES字段类型
					EsAnalyzer: v.EsAnalyzer,
				}
				esIndexMappings[i] = m
			}
			p.EsIndexMappings = esIndexMappings
		}

		if s.EndpointType == config.EndpointTypeRocketMQ ||
			s.EndpointType == config.EndpointTypeKafka ||
			s.EndpointType == config.EndpointTypeRabbitMQ {
			p.MqTopic = s.MqTopic
			p.MqReserveRawData = s.MqReserveRawData
		}

		if s.EndpointType == config.EndpointTypeHttp {
			p.HttpParameterName = s.HttpParameterName
			p.HttpReserveRawData = s.HttpReserveRawData
		}
	}

	return p
}

func (s *TransformRuleVO) FromPO(p *po.TransformRule) {
	s.Type = p.Type
	s.TypeName = "规则"
	if p.Type == config.TransformRuleLuaScript {
		s.TypeName = "脚本"
	}
	s.EndpointType = p.EndpointType
	s.Schema = p.Schema
	s.Table = p.Table
	s.PipelineInfoId = p.PipelineInfoId
	s.Key = fmt.Sprintf("%s.%s", p.Schema, p.Table)
	if p.Type == config.TransformRuleLuaScript {
		s.LuaScript = p.LuaScript
	} else {
		s.ReceiveType = stringutils.ToString(p.ReceiveType)
		s.ColumnNameFormatter = stringutils.ToString(p.ColumnNameFormatter) //列名转换格式 0:列名称转为小写 1:列名称转为大写 2:列名称下划线转驼峰
		if p.ExcludeColumnList != nil {
			s.ExcludeColumnList = p.ExcludeColumnList // 排除掉的列
		} else {
			s.ExcludeColumnList = make([]string, 0) // 排除掉的列
		}
		s.DataEncoder = stringutils.ToString(p.DataEncoder)             //数据编码类型，0: json、 1:表达式
		s.DataExpression = p.DataExpression                             //数据expression
		s.DateFormatter = stringutils.ToString(p.DateFormatter)         //date类型格式化
		s.DatetimeFormatter = stringutils.ToString(p.DatetimeFormatter) //datetime、timestamp类型格式化
		s.OrderColumn = p.OrderColumn

		if nil != p.ColumnNameMapping && len(p.ColumnNameMapping) > 0 {
			columnMappingGroups := make([]ColumnMappingItem, len(p.ColumnNameMapping))
			index := 0
			for k, v := range p.ColumnNameMapping {
				item := ColumnMappingItem{
					Column:  k,
					Mapping: v,
				}
				columnMappingGroups[index] = item
				index++
			}
			s.ColumnMappingGroups = columnMappingGroups
		} else {
			s.ColumnMappingGroups = make([]ColumnMappingItem, 0)
		}

		if nil != p.AdditionalColumnValueMapping && len(p.AdditionalColumnValueMapping) > 0 {
			additionalColumnValueMappingGroups := make([]AdditionalColumnValueItem, len(p.AdditionalColumnValueMapping))
			index := 0
			for k, v := range p.AdditionalColumnValueMapping {
				item := AdditionalColumnValueItem{
					Column: k,
					Value:  v,
				}
				additionalColumnValueMappingGroups[index] = item
				index++
			}
			s.AdditionalColumnValueMappingGroups = additionalColumnValueMappingGroups
		} else {
			s.AdditionalColumnValueMappingGroups = make([]AdditionalColumnValueItem, 0)
		}

		if p.EndpointType == config.EndpointTypeRedis {
			s.RedisStructure = stringutils.ToString(p.RedisStructure)   //对应redis的5种数据类型 1:String、2:Hash(字典) 、3:List(列表) 、4:Set(集合)、5:Sorted Set(有序集合)
			s.RedisKeyPrefix = p.RedisKeyPrefix                         //key的前缀
			s.RedisKeyBuilder = stringutils.ToString(p.RedisKeyBuilder) //key生成方式，0:使用列值(默认使用主键)、1:表达式、2固定值
			s.RedisKeyColumn = p.RedisKeyColumn                         //key生成方式，使用列值，默认使用主键
			s.RedisKeyExpression = p.RedisKeyExpression                 // key生成表达式
			s.RedisKeyFixValue = p.RedisKeyFixValue                     // key固定值
			s.RedisHashFieldPrefix = p.RedisHashFieldPrefix             // hash的field前缀，仅redis_structure为hash时起作用
			s.RedisHashFieldColumn = p.RedisHashFieldColumn             // 使用哪个列的值作为hash的field，仅redis_structure为hash时起作用
			s.RedisSortedSetScoreColumn = p.RedisSortedSetScoreColumn   // Sorted Set(有序集合)的Score
		}

		if p.EndpointType == config.EndpointTypeMongoDB {
			s.MongodbDatabase = p.MongodbDatabase
			s.MongodbCollection = p.MongodbCollection
		}

		if p.EndpointType == config.EndpointTypeElasticsearch {
			s.EsIndexBuildType = stringutils.ToString(p.EsIndexBuildType) //Index名称创建方式，0使用已经存在的、1自动创建
			s.EsIndexName = p.EsIndexName
			if nil != p.EsIndexMappings && len(p.EsIndexMappings) > 0 {
				esIndexMappingGroups := make([]EsIndexMappingItem, len(p.EsIndexMappings))
				for i, v := range p.EsIndexMappings {
					m := EsIndexMappingItem{
						Column:     v.Column,  // 数据库列名称
						EsField:    v.EsField, // 映射后的ES字段名称
						EsType:     v.EsType,  // ES字段类型
						EsAnalyzer: v.EsAnalyzer,
					}
					esIndexMappingGroups[i] = m
				}
				s.EsIndexMappingGroups = esIndexMappingGroups
			}
		}

		if p.EndpointType == config.EndpointTypeRocketMQ ||
			p.EndpointType == config.EndpointTypeKafka ||
			p.EndpointType == config.EndpointTypeRabbitMQ {
			s.MqTopic = p.MqTopic
			s.MqReserveRawData = p.MqReserveRawData
		}

		if p.EndpointType == config.EndpointTypeHttp {
			s.HttpParameterName = p.HttpParameterName
			s.HttpReserveRawData = p.HttpReserveRawData
		}

	}
}

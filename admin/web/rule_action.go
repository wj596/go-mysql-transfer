package web

import (
	"fmt"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/schema"
	"github.com/yuin/gopher-lua"
	"github.com/yuin/gopher-lua/parse"

	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/vo"
	"go-mysql-transfer/service"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/stringutils"
)

type TransformRuleAction struct {
	service         *service.TransformRuleService
	sourceService   *service.SourceInfoService
	endpointService *service.EndpointInfoService
	pipelineService *service.PipelineInfoService
}

func initTransformRuleAction(r *gin.Engine) {
	s := &TransformRuleAction{
		service:         service.GetTransformRuleService(),
		sourceService:   service.GetSourceInfoService(),
		endpointService: service.GetEndpointInfoService(),
		pipelineService: service.GetPipelineInfoService(),
	}
	r.GET("rules", s.Select)
	r.POST("rules/validate", s.Validate)
	r.GET("rules/:id", s.GetBy)
}

func (s *TransformRuleAction) Select(c *gin.Context) {
	pipelineId := stringutils.ToUint64Safe(c.Query("pipelineId"))
	endpointType := stringutils.ToInt32Safe(c.Query("endpointType"))
	isCascadePipeline := stringutils.ToBoolSafe(c.Query("isCascadePipeline"))

	list, err := s.service.SelectList(pipelineId, endpointType)
	if nil != err {
		log.Errorf("获取数据失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}

	results := make([]*vo.TransformRuleVO, len(list))
	for i, v := range list {
		temp := new(vo.TransformRuleVO)
		temp.FromPO(v)
		if isCascadePipeline {
			if vv, err := s.pipelineService.Get(v.PipelineInfoId); err == nil {
				temp.PipelineInfoName = vv.Name
			}
		}
		results[i] = temp
	}

	RespData(c, results)
}

func (s *TransformRuleAction) GetBy(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("id"))
	data, err := s.service.Get(id)
	if nil != err {
		log.Errorf("获取数据失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}

	vo := new(vo.TransformRuleVO)
	vo.FromPO(data)

	RespData(c, vo)
}

func (s *TransformRuleAction) Validate(c *gin.Context) {
	entity := new(vo.TransformRuleVO)
	if err := c.BindJSON(entity); err != nil {
		log.Errorf("验证失败: %s", errors.ErrorStack(err))
		Err400(c, err.Error())
		return
	}

	fmt.Println("Validate: \n", stringutils.ToJsonIndent(entity))

	tableInfo, err := s.sourceService.SelectTableInfo(entity.SourceId, entity.Schema, entity.Table)
	if err != nil {
		log.Errorf("验证失败: %s", errors.ErrorStack(err))
		Err400(c, err.Error())
		return
	}

	endpoint, err := s.endpointService.Get(entity.EndpointId)
	if err != nil {
		log.Errorf("验证失败: %s", errors.ErrorStack(err))
		Err400(c, err.Error())
		return
	}

	if len(tableInfo.Columns) == len(entity.ExcludeColumnList) {
		Err400(c, "不能排除所有的列！！！")
		return
	}

	columnMap := make(map[string]*bo.TableColumnInfo) // 列
	for _, column := range tableInfo.Columns {
		columnMap[column.Name] = column
	}

	excludeColumnMap := make(map[string]int) // 排除的列
	for _, exclude := range entity.ExcludeColumnList {
		if entity.IsCopy {
			if _, exist := columnMap[exclude]; !exist {
				Err400(c, "要拷贝规则中'排除的列："+exclude+"'，不是目标Table中的有效列，无法完成拷贝，请重新选择！！！")
				return
			}
		}
		excludeColumnMap[exclude] = 1
	}

	if entity.ColumnMappingGroups != nil {
		columnMappingMap := make(map[string]int)
		mappingColumnMap := make(map[string]int)
		for _, item := range entity.ColumnMappingGroups {
			if entity.IsCopy {
				if _, exist := columnMap[item.Column]; !exist {
					Err400(c, "要拷贝规则中'列名映射："+item.Column+"'，不是目标Table中的有效列，无法完成拷贝，请重新选择！！！")
					return
				}
			}

			if _, exist := excludeColumnMap[item.Column]; exist {
				Err400(c, "'列名映射'不能映射已经排除的列,请重新选择！！！")
				return
			}

			if _, exist := columnMappingMap[item.Column]; exist {
				Err400(c, "'列名映射'中存在重复的列名,请重新选择！！！")
				return
			} else {
				columnMappingMap[item.Column] = 1
			}

			if _, exist := mappingColumnMap[item.Mapping]; exist {
				Err400(c, "'列名映射'中存在重复的映射名,请重新输入！！！")
				return
			} else {
				mappingColumnMap[item.Mapping] = 1
			}
		}
	}

	if entity.AdditionalColumnValueMappingGroups != nil {
		for _, item := range entity.AdditionalColumnValueMappingGroups {
			for _, column := range tableInfo.Columns {
				if strings.ToLower(item.Column) == strings.ToLower(column.Name) {
					if entity.IsCopy {
						Err400(c, "要拷贝规则中'附加键值："+item.Column+"'，是目标Table中已经存在的列名，无法完成拷贝，请重新选择！！！")
						return
					}
					Err400(c, "'附加键值'中附加列的名称不能为已经存在的列名,请重新输入！！！")
					return
				}
			}
		}
	}

	if endpoint.Type == constants.EndpointTypeRedis {
		if entity.RedisKeyColumn != "" {
			if entity.IsCopy {
				if _, exist := columnMap[entity.RedisKeyColumn]; !exist {
					Err400(c, "要拷贝规则中'key列："+entity.RedisKeyColumn+"'，不是目标Table中的有效列，无法完成拷贝，请重新选择！！！")
					return
				}
			}
			if _, exist := excludeColumnMap[entity.RedisKeyColumn]; exist {
				Err400(c, "已排除的列不能作为'key列',请重选择！！！")
				return
			}
		}
		if entity.RedisHashFieldColumn != "" {
			if entity.IsCopy {
				if _, exist := columnMap[entity.RedisHashFieldColumn]; !exist {
					Err400(c, "要拷贝规则中'field列："+entity.RedisHashFieldColumn+"'，不是目标Table中的有效列，无法完成拷贝，请重新选择！！！")
					return
				}
			}
			if _, exist := excludeColumnMap[entity.RedisHashFieldColumn]; exist {
				Err400(c, "已排除的列不能作为'field列',请重选择！！！")
				return
			}
		}
		if entity.RedisSortedSetScoreColumn != "" {
			if entity.IsCopy {
				if _, exist := columnMap[entity.RedisSortedSetScoreColumn]; !exist {
					Err400(c, "要拷贝规则中'score列："+entity.RedisSortedSetScoreColumn+"'，不是目标Table中的有效列，无法完成拷贝，请重新选择！！！")
					return
				}
			}
			if _, exist := excludeColumnMap[entity.RedisSortedSetScoreColumn]; exist {
				Err400(c, "已排除的列不能作为'score列',请重选择！！！")
				return
			}
			column, _ := columnMap[entity.RedisSortedSetScoreColumn]
			if !(column.Type == schema.TYPE_NUMBER || column.Type == schema.TYPE_FLOAT || column.Type == schema.TYPE_DECIMAL) {
				Err400(c, "'score列'必须为数值类型,请重选择！！！")
				return
			}
		}
	}

	if endpoint.Type == constants.EndpointTypeElasticsearch {
		if entity.EsIndexMappingGroups != nil {
			columnMappingMap := make(map[string]int)
			mappingColumnMap := make(map[string]int)
			for _, item := range entity.EsIndexMappingGroups {
				if entity.IsCopy {
					if _, exist := columnMap[item.Column]; !exist {
						Err400(c, "要拷贝规则中'索引Mapping："+item.Column+"'，不是目标Table中的有效列，无法完成拷贝，请重新选择！！！")
						return
					}
				}

				if _, exist := excludeColumnMap[item.Column]; exist {
					Err400(c, "'索引Mapping'不能包含已经排除的列,请重新选择！！！")
					return
				}

				if _, exist := columnMappingMap[item.Column]; exist {
					Err400(c, "'索引Mapping'中存在重复的列名,请重新选择！！！")
					return
				} else {
					columnMappingMap[item.Column] = 1
				}

				if _, exist := mappingColumnMap[item.EsField]; exist {
					Err400(c, "'索引Mapping'中存在重复的ES字段名,请重新输入！！！")
					return
				} else {
					mappingColumnMap[item.EsField] = 1
				}
			}
		}

		if entity.EsIndexBuildType == constants.EsIndexBuildTypeExtend {
			// 检查ES中是否存在索引
		}

		if entity.EsIndexBuildType == constants.EsIndexBuildTypeAutoCreate {
			// 创建或者更新索引
		}
	}

	if constants.TransformRuleTypeLuaScript == entity.Type {
		protoName := stringutils.UUID()
		reader := strings.NewReader(entity.LuaScript)
		chunk, err := parse.Parse(reader, protoName)
		if err != nil {
			log.Errorf("验证失败,Lua脚本错误，无法编译: %s", errors.ErrorStack(err))
			Err400(c, "Lua脚本编译失败："+err.Error())
			return
		}
		_, err = lua.Compile(chunk, protoName)
		if err != nil {
			log.Errorf("验证失败,Lua脚本错误，无法编译: %s", errors.ErrorStack(err))
			Err400(c, "Lua脚本编译失败："+err.Error())
			return
		}
		chunk = nil
	}

	RespOK(c)
}

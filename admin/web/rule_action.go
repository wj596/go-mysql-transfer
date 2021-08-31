package web

import (
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/juju/errors"

	"go-mysql-transfer/config"
	"go-mysql-transfer/model/vo"
	"go-mysql-transfer/service"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/stringutils"
)

type TransformRuleAction struct {
	service         *service.TransformRuleService
	sourceService   *service.SourceInfoService
	endpointService *service.EndpointInfoService
}

func initTransformRuleAction(r *gin.Engine) {
	s := &TransformRuleAction{
		service:         service.GetTransformRuleService(),
		sourceService:   service.GetSourceInfoService(),
		endpointService: service.GetEndpointInfoService(),
	}
	r.GET("rules", s.Select)
	r.POST("rules/validate", s.Validate)
	r.GET("rules/by_id/:id", s.GetBy)
}

func (s *TransformRuleAction) Select(c *gin.Context) {
	pipelineId := stringutils.ToUint64Safe(c.Query("pipelineId"))
	endpointType := stringutils.ToInt32Safe(c.Query("endpointType"))

	data, err := s.service.SelectList(pipelineId, endpointType)
	if nil != err {
		log.Errorf("获取数据失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}

	RespData(c, data)
}

func (s *TransformRuleAction) GetBy(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("id"))
	vo, err := s.service.Get(id)
	if nil != err {
		log.Errorf("获取数据失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}
	RespData(c, vo)
}

func (s *TransformRuleAction) Validate(c *gin.Context) {
	entity := new(vo.TransformRuleVO)
	if err := c.BindJSON(entity); err != nil {
		log.Errorf("验证失败: %s", errors.ErrorStack(err))
		Err400(c, err.Error())
		return
	}

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

	excludeColumnMap := make(map[string]int)
	for _, exclude := range entity.ExcludeColumnList {
		excludeColumnMap[exclude] = 1
	}

	if entity.ColumnMappingGroups != nil {
		columnMappingMap := make(map[string]int)
		mappingColumnMap := make(map[string]int)
		for _, item := range entity.ColumnMappingGroups {
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
				if strings.ToLower(item.Column) == column.Name {
					Err400(c, "'附加键值'中附加列的名称不能为已经存在的列名,请重新输入！！！")
					return
				}
			}
		}
	}

	if endpoint.Type == config.EndpointTypeRedis {
		if entity.RedisKeyColumn != "" {
			if _, exist := excludeColumnMap[entity.RedisKeyColumn]; exist {
				Err400(c, "已排除的列不能作为'key列',请重选择！！！")
				return
			}
		}
		if entity.RedisHashFieldColumn != "" {
			if _, exist := excludeColumnMap[entity.RedisKeyColumn]; exist {
				Err400(c, "已排除的列不能作为'field列',请重选择！！！")
				return
			}
		}
	}

	if endpoint.Type == config.EndpointTypeElasticsearch {
		if entity.EsIndexMappingGroups != nil {
			columnMappingMap := make(map[string]int)
			mappingColumnMap := make(map[string]int)
			for _, item := range entity.EsIndexMappingGroups {
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

		if entity.EsIndexBuildType == config.EsIndexBuildTypeExtend {
			// 检查ES中是否存在索引
		}

		if entity.EsIndexBuildType == config.EsIndexBuildTypeAutoCreate {
			// 创建或者更新索引
		}
	}

	RespOK(c)
}

/*
 * Copyright 2021-2022 the original author(https://github.com/wj596)
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

package web

import (
	"fmt"
	"strings"

	"github.com/asaskevich/govalidator"
	"github.com/gin-gonic/gin"
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/schema"
	"github.com/yuin/gopher-lua"
	"github.com/yuin/gopher-lua/parse"

	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/domain/vo"
	"go-mysql-transfer/service"
	"go-mysql-transfer/util/commons"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/stringutils"
)

type PipelineInfoAction struct {
	service         *service.PipelineInfoService
	sourceService   *service.SourceInfoService
	endpointService *service.EndpointInfoService
}

func initPipelineInfoAction(r *gin.RouterGroup) {
	s := &PipelineInfoAction{
		service:         service.GetPipelineInfoService(),
		sourceService:   service.GetSourceInfoService(),
		endpointService: service.GetEndpointInfoService(),
	}

	r.POST("pipelines", s.Insert)
	r.POST("pipelines/validate-rule", s.ValidateRule)
	r.PUT("pipelines", s.Update)
	r.PUT("pipelines/:id/enable", s.Enable)
	r.PUT("pipelines/:id/disable", s.Disable)
	r.DELETE("pipelines/:id", s.DeleteBy)
	r.GET("pipelines/entity/:id", s.GetBy)
	r.GET("pipelines", s.Select)
	r.GET("pipelines/rules", s.SelectRules)
}

func (s *PipelineInfoAction) Insert(c *gin.Context) {
	vo := new(vo.PipelineInfoVO)
	if err := c.BindJSON(vo); err != nil {
		log.Errorf("新增失败: %s", errors.ErrorStack(err))
		Err400(c, err.Error())
		return
	}

	if err := s.validate(vo, false); err != nil {
		log.Errorf("新增失败: %s", errors.ErrorStack(err))
		Err400(c, err.Error())
		return
	}

	entity := vo.ToPO()
	rules := make([]*po.Rule, len(vo.Rules))
	for i, v := range vo.Rules {
		vv := v.ToPO(entity.EndpointType)
		rules[i] = vv
	}
	entity.Rules = rules

	if err := s.service.Insert(entity); err != nil {
		log.Errorf("新增失败: %s", errors.ErrorStack(err))
		Err500(c, err.Error())
		return
	}

	RespOK(c)
}

func (s *PipelineInfoAction) Update(c *gin.Context) {
	vo := new(vo.PipelineInfoVO)
	if err := c.BindJSON(vo); err != nil {
		log.Errorf("更新失败: %s", errors.ErrorStack(err))
		Err400(c, err.Error())
		return
	}

	if err := s.validate(vo, true); err != nil {
		log.Errorf("更新失败: %s", errors.ErrorStack(err))
		Err400(c, err.Error())
		return
	}

	entity := vo.ToPO()
	rules := make([]*po.Rule, len(vo.Rules))
	for i, v := range vo.Rules {
		vv := v.ToPO(entity.EndpointType)
		rules[i] = vv
	}
	entity.Rules = rules

	if err := s.service.UpdateEntity(entity); err != nil {
		log.Errorf("更新失败: %s", errors.ErrorStack(err))
		Err500(c, err.Error())
		return
	}

	RespOK(c)
}

func (s *PipelineInfoAction) Enable(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("id"))
	err := s.service.UpdateStatus(id, constants.PipelineInfoStatusEnable)
	if err != nil {
		log.Errorf("更新失败: %s", errors.ErrorStack(err))
		Err500(c, err.Error())
		return
	}
	RespOK(c)
}

func (s *PipelineInfoAction) Disable(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("id"))
	err := s.service.UpdateStatus(id, constants.PipelineInfoStatusDisable)
	if err != nil {
		log.Errorf("更新失败: %s", errors.ErrorStack(err))
		Err500(c, err.Error())
		return
	}
	RespOK(c)
}

func (s *PipelineInfoAction) DeleteBy(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("id"))
	if err := s.service.Delete(id); err != nil {
		log.Errorf("删除失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}
	RespOK(c)
}

func (s *PipelineInfoAction) GetBy(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("id"))
	entity, err := s.service.Get(id)
	if nil != err {
		log.Errorf("获取数据失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}
	v := new(vo.PipelineInfoVO)
	v.FromPO(entity)
	s.padding(v)
	rules := make([]*vo.RuleVO, 0, len(entity.Rules))
	for _, r := range entity.Rules {
		rule := new(vo.RuleVO)
		rule.FromPO(r, entity.EndpointType)
		rule.PipelineName = entity.Name
		rules = append(rules, rule)
	}
	v.Rules = rules
	RespData(c, v)
}

func (s *PipelineInfoAction) Select(c *gin.Context) {
	params := &vo.PipelineInfoParams{
		Name: c.Query("name"),
	}
	items, err := s.service.SelectList(params)
	if nil != err {
		log.Errorf("获取数据失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}

	vs := make([]*vo.PipelineInfoVO, 0, len(items))
	for _, item := range items {
		v := new(vo.PipelineInfoVO)
		v.FromPO(item)
		s.padding(v)
		vs = append(vs, v)
	}
	RespData(c, vs)
}

func (s *PipelineInfoAction) SelectRules(c *gin.Context) {
	endpointType := stringutils.ToUint32Safe(c.Param("endpointType"))
	params := &vo.PipelineInfoParams{
		EndpointType: endpointType,
	}
	pipes, err := s.service.SelectList(params)
	if nil != err {
		log.Errorf("获取数据失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}
	rules := make([]*vo.RuleVO, 0)
	for _, pipe := range pipes {
		for _, r := range pipe.Rules {
			rule := new(vo.RuleVO)
			rule.FromPO(r, pipe.EndpointType)
			rule.PipelineId = pipe.Id
			rule.PipelineName = pipe.Name
			rules = append(rules, rule)
		}
	}

	RespData(c, rules)
}

func (s *PipelineInfoAction) validate(pipeline *vo.PipelineInfoVO, update bool) error {
	params := vo.NewPipelineInfoParams()
	params.Name = pipeline.Name
	exist, _ := s.service.GetByParam(params)
	if !update {
		if exist != nil {
			return errors.Errorf("存在名称为[%s]的通道，请更换", pipeline.Name)
		}
		params.Name = ""
		params.SourceId = pipeline.SourceId
		params.EndpointId = pipeline.EndpointId
		entity, _ := s.service.GetByParam(params)
		if entity != nil {
			vvo := new(vo.PipelineInfoVO)
			vvo.FromPO(entity)
			s.padding(vvo)
			return errors.Errorf("存在数据源为'%s'，接收端点为'%s'的通道，无需重复创建", vvo.SourceName, vvo.EndpointName)
		}
	} else {
		if exist != nil && exist.Id != pipeline.Id {
			return errors.Errorf("存在名称为[%s]的通道，请更换", pipeline.Name)
		}
	}

	if pipeline.AlarmMailList != "" {
		mails := strings.Split(pipeline.AlarmMailList, ",")
		for _, mail := range mails {
			if !govalidator.IsEmail(mail) {
				return errors.Errorf("'告警邮箱地址'中存在不合规的邮箱地址[%s]", mail)
			}
		}
	}

	if pipeline.AlarmWebhook != "" {
		if !govalidator.IsURL(pipeline.AlarmWebhook) {
			return errors.Errorf("'告警钉钉Webhook地址'不是有效的URL地址[%s]", pipeline.AlarmWebhook)
		}
	}

	return nil
}

func (s *PipelineInfoAction) ValidateRule(c *gin.Context) {
	entity := new(vo.RuleVO)
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

	if constants.RuleTypeLuaScript == entity.Type {
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

func (s *PipelineInfoAction) padding(v *vo.PipelineInfoVO) {
	if source, err := s.sourceService.Get(v.SourceId); err == nil {
		v.SourceName = fmt.Sprintf("%s[%s:%d]", source.Name, source.Host, source.Port)
	}
	if endpoint, err := s.endpointService.Get(v.EndpointId); err == nil {
		v.EndpointName = fmt.Sprintf("%s[%s %s]", endpoint.Name, commons.GetEndpointTypeName(endpoint.Type), endpoint.Addresses)
	}
}

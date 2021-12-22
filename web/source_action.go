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

	"github.com/asaskevich/govalidator"
	"github.com/gin-gonic/gin"
	"github.com/juju/errors"

	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/domain/vo"
	"go-mysql-transfer/service"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/stringutils"
)

type SourceInfoAction struct {
	service         *service.SourceInfoService
	pipelineService *service.PipelineInfoService
	stateService    *service.PipelineStateService
}

func initSourceInfoAction(r *gin.RouterGroup) {
	s := &SourceInfoAction{
		service:         service.GetSourceInfoService(),
		pipelineService: service.GetPipelineInfoService(),
		stateService:    service.GetStateService(),
	}
	r.POST("sources", s.Insert)
	r.POST("sources/test-connect", s.TestConnect)
	r.PUT("sources", s.Update)
	r.DELETE("sources/:id", s.DeleteBy)
	r.GET("sources/:id", s.GetBy)
	r.GET("sources", s.Select)
	r.GET("sources/:id/is-running", s.IsRunning)
	r.GET("sources/:id/schemas", s.SelectSchemaList)
	r.GET("sources/:id/tables", s.SelectTableList)
	r.GET("sources/:id/table-info", s.GetTableInfo)
}

func (s *SourceInfoAction) Insert(c *gin.Context) {
	vo := new(vo.SourceInfoVO)

	if err := c.BindJSON(vo); err != nil {
		log.Errorf("新增失败: %s", errors.ErrorStack(err))
		Err400(c, err.Error())
		return
	}

	if err := s.check(vo, false); err != nil {
		log.Errorf("新增失败: %s", errors.ErrorStack(err))
		Err400(c, err.Error())
		return
	}

	if err := s.service.Insert(vo.ToPO()); err != nil {
		log.Errorf("新增失败: %s", errors.ErrorStack(err))
		Err500(c, err.Error())
		return
	}

	RespOK(c)
}

func (s *SourceInfoAction) Update(c *gin.Context) {
	vo := new(vo.SourceInfoVO)

	if err := c.BindJSON(vo); err != nil {
		log.Errorf("更新失败: %s", errors.ErrorStack(err))
		Err400(c, err.Error())
		return
	}

	if err := s.check(vo, true); err != nil {
		log.Errorf("更新失败: %s", errors.ErrorStack(err))
		Err400(c, err.Error())
		return
	}

	if err := s.service.Update(vo.ToPO()); err != nil {
		log.Errorf("更新失败: %s", errors.ErrorStack(err))
		Err500(c, err.Error())
		return
	}

	RespOK(c)
}

func (s *SourceInfoAction) DeleteBy(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("id"))

	if pipe, err := s.pipelineService.GetByParam(&vo.PipelineInfoParams{
		SourceId: id,
	}); err == nil {
		msg := fmt.Sprintf("管道[%s] 正在使用该数据源，先删除管道[%s]后才能删除此数据源", pipe.Name, pipe.Name)
		log.Errorf("删除失败:%s", msg)
		Err500(c, msg)
		return
	}

	if err := s.service.Delete(id); err != nil {
		log.Errorf("删除失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}

	RespOK(c)
}

func (s *SourceInfoAction) GetBy(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("id"))
	po, err := s.service.Get(id)

	if nil != err {
		log.Errorf("获取数据失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}

	vo := new(vo.SourceInfoVO)
	vo.FromPO(po)

	RespData(c, vo)
}

func (s *SourceInfoAction) Select(c *gin.Context) {
	params := &vo.SourceInfoParams{
		Name:   c.Query("name"),
		Host:   c.Query("host"),
		Enable: stringutils.ToBoolSafe(c.Query("enable")),
	}
	list, err := s.service.SelectList(params)
	if nil != err {
		log.Errorf("获取数据失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}

	vos := make([]*vo.SourceInfoVO, 0, len(list))
	for _, l := range list {
		vo := new(vo.SourceInfoVO)
		vo.FromPO(l)
		vos = append(vos, vo)
	}

	RespData(c, vos)
}

func (s *SourceInfoAction) IsRunning(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("id"))
	params := &vo.PipelineInfoParams{
		SourceId: id,
	}
	pipelines, err := s.pipelineService.SelectList(params)
	if nil != err {
		log.Errorf("获取数据失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}

	var running bool
	for _, pipeline := range pipelines {
		if service.IsClusterAndLeader() { //集群
			state, _ := s.stateService.GetState(pipeline.Id)
			if state != nil {
				if constants.PipelineRunStatusRunning == state.Status ||
					constants.PipelineRunStatusFault == state.Status ||
					constants.PipelineRunStatusBatching == state.Status {
					running = true
					break
				}
			}
		} else { //单机
			if runtime, _ := s.stateService.GetOrCreateRuntime(pipeline.Id); nil != runtime {
				if constants.PipelineRunStatusRunning == runtime.Status.Load() ||
					constants.PipelineRunStatusFault == runtime.Status.Load() ||
					constants.PipelineRunStatusBatching == runtime.Status.Load() {
					running = true
					break
				}
			}
		}
	}

	RespData(c, running)
}

func (s *SourceInfoAction) SelectSchemaList(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("id"))
	data, err := s.service.SelectSchemaList(id)
	if nil != err {
		log.Errorf("获取数据失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}

	RespData(c, data)
}

func (s *SourceInfoAction) SelectTableList(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("id"))
	schema := c.Query("schema")

	data, err := s.service.SelectTableList(id, schema)
	if nil != err {
		log.Errorf("获取数据失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}

	RespData(c, data)
}

func (s *SourceInfoAction) GetTableInfo(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("id"))
	schema := c.Query("schema")
	table := c.Query("table")

	data, err := s.service.SelectTableInfo(id, schema, table)
	if nil != err {
		log.Errorf("获取数据失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}

	RespData(c, data)
}

func (s *SourceInfoAction) TestConnect(c *gin.Context) {
	vo := new(po.SourceInfo)
	if err := c.BindJSON(vo); err != nil {
		log.Errorf("链接测试失败: %s", errors.ErrorStack(err))
		Err400(c, err.Error())
		return
	}

	if err := s.service.TestConnect(vo); err != nil {
		log.Errorf("链接测试失败: %s", errors.ErrorStack(err))
		Err500(c, fmt.Sprintf("链接失败：%s", err.Error()))
		return
	}

	RespOK(c)
}

func (s *SourceInfoAction) check(v *vo.SourceInfoVO, isUpdate bool) error {
	if !govalidator.IsHost(v.Host) {
		return errors.New("主机Host格式不正确")
	}

	list, err := s.service.SelectList(vo.NewSourceInfoParams())
	if err != nil {
		return err
	}

	mark := stringutils.Join(v.Host, v.Port, v.SlaveID)
	for _, item := range list {
		if item.Name == v.Name && item.Id != v.Id {
			return errors.New(fmt.Sprintf("存在名称为[%s]的数据源，请更换", v.Name))
		}

		itemMark := stringutils.Join(item.Host, item.Port, item.SlaveID)
		if itemMark == mark && item.Id != v.Id {
			return errors.New(fmt.Sprintf("此库存在相同的SlaveID[%d]，请更换SlaveID", v.SlaveID))
		}
	}

	if isUpdate && v.Status == constants.SourceInfoStatusDisable {
		if pipe, err := s.pipelineService.GetByParam(&vo.PipelineInfoParams{
			SourceId: v.Id,
		}); err == nil {
			return errors.Errorf("管道[%s] 正在使用该数据源，先删除管道[%s]后才能停用此数据源", pipe.Name, pipe.Name)
		}
	}

	return nil
}

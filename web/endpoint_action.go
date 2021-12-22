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

	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/domain/vo"
	"go-mysql-transfer/service"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/stringutils"
)

type EndpointInfoAction struct {
	service         *service.EndpointInfoService
	pipelineService *service.PipelineInfoService
	stateService    *service.PipelineStateService
}

func initEndpointInfoAction(r *gin.RouterGroup) {
	s := &EndpointInfoAction{
		service:         service.GetEndpointInfoService(),
		pipelineService: service.GetPipelineInfoService(),
		stateService:    service.GetStateService(),
	}
	r.POST("endpoints", s.Insert)
	r.POST("endpoints/test-link", s.TestLink)
	r.PUT("endpoints", s.Update)
	r.DELETE("endpoints/:id", s.DeleteBy)
	r.GET("endpoints/:id", s.GetBy)
	r.GET("endpoints", s.Select)
	r.GET("endpoints/:id/is-running", s.IsRunning)
}

func (s *EndpointInfoAction) Insert(c *gin.Context) {
	vo := new(vo.EndpointInfoVO)
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

func (s *EndpointInfoAction) Update(c *gin.Context) {
	vo := new(vo.EndpointInfoVO)
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

func (s *EndpointInfoAction) DeleteBy(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("id"))

	if pipe, err := s.pipelineService.GetByParam(&vo.PipelineInfoParams{
		EndpointId: id,
	}); err == nil {
		msg := fmt.Sprintf("管道[%s] 正在使用该端点，先删除管道[%s]后才能删除此端点", pipe.Name, pipe.Name)
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

func (s *EndpointInfoAction) GetBy(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("id"))
	po, err := s.service.Get(id)
	if nil != err {
		log.Errorf("获取数据失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}

	vo := new(vo.EndpointInfoVO)
	vo.FromPO(po)

	RespData(c, vo)
}

func (s *EndpointInfoAction) Select(c *gin.Context) {
	params := &vo.EndpointInfoParams{
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

	vos := make([]*vo.EndpointInfoVO, 0, len(list))
	for _, l := range list {
		vo := new(vo.EndpointInfoVO)
		vo.FromPO(l)
		vos = append(vos, vo)
	}

	RespData(c, vos)
}

func (s *EndpointInfoAction) TestLink(c *gin.Context) {
	vo := new(po.EndpointInfo)
	if err := c.BindJSON(vo); err != nil {
		log.Errorf("链接测试失败: %s", errors.ErrorStack(err))
		Err400(c, err.Error())
		return
	}

	if err := s.service.TestLink(vo); err != nil {
		log.Errorf("链接测试失败: %s", errors.ErrorStack(err))
		Err500(c, fmt.Sprintf("链接失败：%s", err.Error()))
		return
	}
	RespOK(c)
}

func (s *EndpointInfoAction) IsRunning(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("id"))
	params := &vo.PipelineInfoParams{
		EndpointId: id,
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
			if runtime, _ := s.stateService.GetOrCreateRuntime(pipeline.Id); nil!=runtime {
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

func (s *EndpointInfoAction) check(v *vo.EndpointInfoVO, isUpdate bool) error {
	if constants.EndpointTypeRabbitMQ == v.Type || constants.EndpointTypeHttp == v.Type {
		if !govalidator.IsURL(v.Addresses) {
			return errors.Errorf("地址[%s]格式不正确,请填写正确的连接地址", v.Addresses)
		}
	} else {
		arrays := strings.Split(v.Addresses, ",")
		for _, address := range arrays {
			if !govalidator.IsURL(address) {
				return errors.Errorf("地址[%s]格式不正确,请填写正确的连接地址", address)
			}
		}
	}

	exist, _ := s.service.GetByName(v.Name)
	if exist != nil {
		if !isUpdate {
			return errors.New(fmt.Sprintf("存在名称为[%s]的端点，请更换", v.Name))
		}
		if isUpdate && exist.Id != v.Id {
			return errors.New(fmt.Sprintf("存在名称为[%s]的端点，请更换", v.Name))
		}
	}

	if isUpdate && v.Status == constants.EndpointInfoStatusDisable {
		if pipe, err := s.pipelineService.GetByParam(&vo.PipelineInfoParams{
			EndpointId: v.Id,
		}); err == nil {
			return errors.Errorf("管道[%s] 正在使用该端点，先删除管道[%s]后才能停用此端点", pipe.Name, pipe.Name)
		}
	}

	return nil
}

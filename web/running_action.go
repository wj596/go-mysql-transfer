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
	"github.com/gin-gonic/gin"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/juju/errors"

	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/vo"
	"go-mysql-transfer/service"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/stringutils"
)

type RunningAction struct {
	pipelineService *service.PipelineInfoService
	stateService    *service.PipelineStateService
}

func initRunningAction(r *gin.RouterGroup) {
	s := &RunningAction{
		pipelineService: service.GetPipelineInfoService(),
		stateService:    service.GetStateService(),
	}

	r.GET("runnings", s.Select)
	r.GET("runnings/:id", s.GetBy)
	r.GET("runnings/:id/status", s.GetStatus)
	r.PUT("runnings/:id/start-stream", s.StartStream)
	r.PUT("runnings/:id/stop-stream", s.StopStream)
	r.PUT("runnings/:id/start-batch", s.StartBatch)
	r.PUT("runnings/:id/position", s.SetPosition)
}

func (s *RunningAction) Select(c *gin.Context) {
	params := &vo.PipelineInfoParams{
		Name: c.Query("name"),
	}
	items, err := s.pipelineService.SelectList(params)
	if nil != err {
		log.Errorf("获取数据失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}

	rets := make([]*vo.PipelineRuntimeVO, 0)
	for _, pipeline := range items {
		if pipeline.Status == constants.PipelineInfoStatusDisable {
			continue
		}
		ret := &vo.PipelineRuntimeVO{
			PipelineId:   pipeline.Id,
			PipelineName: pipeline.Name,
		}

		if service.IsClusterAndLeader() { //集群
			if state, _ := s.stateService.GetState(pipeline.Id); nil != state {
				ret.Status = state.Status
				ret.Node = state.Node
				ret.StartTime = state.StartTime
			}
		} else { //单机
			if runtime, _ := s.stateService.GetOrCreateRuntime(pipeline.Id); nil != runtime {
				if runtime.Status != nil {
					ret.Status = runtime.Status.Load()
				}
				if runtime.StartTime != nil {
					ret.StartTime = runtime.StartTime.Load()
				}
			}
		}
		rets = append(rets, ret)
	}

	RespData(c, rets)
}

func (s *RunningAction) GetBy(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("id"))
	if service.IsClusterAndLeader() { //集群
		node, _ := service.GetLeaderService().GetAllocateNode(id)
		if service.GetCurrNode() != node {
			t, err := service.GetLeaderService().GetFollowerRuntime(id, node)
			if err != nil {
				log.Error(err.Error())
			}
			RespData(c, t)
			return
		}
	}

	runtime, err := s.stateService.GetOrCreateRuntime(id)
	if err != nil {
		Err500(c, err.Error())
		return
	}
	RespData(c, runtime.ToVO())
}

func (s *RunningAction) GetStatus(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("id"))
	var status uint32
	status = constants.PipelineRunStatusInitial

	if service.IsClusterAndLeader() { //集群
		if state, _ := s.stateService.GetState(id); nil != state {
			status = state.Status
		}
	} else { //单机
		if runtime, _ := s.stateService.GetOrCreateRuntime(id); nil != runtime {
			status = runtime.Status.Load()
		}
	}

	RespData(c, status)
}

func (s *RunningAction) StartStream(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("id"))

	var err error
	if service.IsClusterAndLeader() {
		err = service.GetLeaderService().StartStream(id)
	} else {
		err = s.pipelineService.StartStream(id)
	}
	if nil != err {
		Err500(c, err.Error())
		return
	}

	RespOK(c)
}

func (s *RunningAction) StopStream(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("id"))

	var err error
	if service.IsClusterAndLeader() {
		err = service.GetLeaderService().StopStream(id)
	} else {
		s.pipelineService.StopStream(id)
	}
	if nil != err {
		Err500(c, err.Error())
		return
	}

	RespOK(c)
}

func (s *RunningAction) StartBatch(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("id"))
	err := s.pipelineService.StartBatch(id)
	if nil != err {
		Err500(c, err.Error())
		return
	}
	RespOK(c)
}

func (s *RunningAction) SetPosition(c *gin.Context) {
	vo := new(vo.PositionVo)
	if err := c.BindJSON(vo); err != nil {
		log.Errorf("更新失败: %s", errors.ErrorStack(err))
		Err400(c, err.Error())
		return
	}

	pipelineId := stringutils.ToUint64Safe(vo.Id)
	pos := mysql.Position{
		Name: vo.File,
		Pos:  vo.Index,
	}

	if err := s.pipelineService.SetPosition(pipelineId, pos); err != nil {
		log.Errorf("更新Position失败: %s", errors.ErrorStack(err))
		Err400(c, err.Error())
		return
	}

	RespOK(c)
}

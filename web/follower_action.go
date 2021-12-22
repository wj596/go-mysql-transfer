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
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/juju/errors"

	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/vo"
	"go-mysql-transfer/service"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/stringutils"
)

type FollowerAction struct {
	pipelineService *service.PipelineInfoService
	stateService    *service.PipelineStateService
}

func initFollowerAction(r *gin.RouterGroup) {
	s := &FollowerAction{
		pipelineService: service.GetPipelineInfoService(),
		stateService:    service.GetStateService(),
	}

	r.GET("followers/:pipelineId/runtime", s.GetRuntime)
	r.POST("followers/handle-sync-event", s.HandleSyncEvent)
	r.PUT("followers/:pipelineId/start-stream", s.StartStream)
	r.PUT("followers/:pipelineId/stop-stream", s.StopStream)
	r.PUT("followers/:pipelineId/start-batch", s.StartBatch)
}

func (s *FollowerAction) GetRuntime(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("pipelineId"))
	var runtime *vo.PipelineRuntimeVO
	if t, _ := s.stateService.GetOrCreateRuntime(id); nil != t {
		runtime = t.ToVO()
	}
	c.JSON(http.StatusOK, runtime)
}

func (s *FollowerAction) HandleSyncEvent(c *gin.Context) {
	vo := new(bo.SyncEvent)
	if err := c.BindJSON(vo); err != nil {
		log.Errorf("新增失败: %s", errors.ErrorStack(err))
		Err400(c, err.Error())
		return
	}
	service.GetFollowerService().AcceptEvent(vo)
	RespOK(c)
}

func (s *FollowerAction) StartStream(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("pipelineId"))
	err := s.pipelineService.StartStream(id)
	if nil != err {
		Err500(c, err.Error())
		return
	}
	RespOK(c)
}

func (s *FollowerAction) StopStream(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("pipelineId"))
	s.pipelineService.StopStream(id)
	RespOK(c)
}

func (s *FollowerAction) StartBatch(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("pipelineId"))
	err := s.pipelineService.StartBatch(id)
	if nil != err {
		Err500(c, err.Error())
		return
	}
	RespOK(c)
}

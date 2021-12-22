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

	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/vo"
	"go-mysql-transfer/service"
)

type DashboardAction struct {
	sourceService   *service.SourceInfoService
	endpointService *service.EndpointInfoService
	pipelineService *service.PipelineInfoService
	stateService    *service.PipelineStateService
}

func initDashboardAction(r *gin.RouterGroup) {
	s := &DashboardAction{
		sourceService:   service.GetSourceInfoService(),
		endpointService: service.GetEndpointInfoService(),
		pipelineService: service.GetPipelineInfoService(),
		stateService:    service.GetStateService(),
	}
	r.GET("dashboards/counts", s.GetCounts)
	r.GET("dashboards/analysis", s.Analysis)
}

func (s *DashboardAction) GetCounts(c *gin.Context) {
	counts := make(map[string]int)
	counts["source"] = 0
	counts["endpoint"] = 0
	counts["pipeline"] = 0

	if ls, err := s.sourceService.SelectList(vo.NewSourceInfoParams()); err == nil {
		counts["source"] = len(ls)
	}

	if ls, err := s.endpointService.SelectList(vo.NewEndpointInfoParams()); err == nil {
		counts["endpoint"] = len(ls)
	}

	pipelines, err := s.pipelineService.SelectList(vo.NewPipelineInfoParams())
	if err == nil {
		counts["pipeline"] = len(pipelines)
	}

	var running int
	for _, pipeline := range pipelines {
		if service.IsClusterAndLeader() { //集群
			if state, _ := s.stateService.GetState(pipeline.Id); state != nil {
				if state.Status == constants.PipelineRunStatusRunning ||
					state.Status == constants.PipelineRunStatusFault ||
					state.Status == constants.PipelineRunStatusBatching {
					running++
				}
			}
		} else { //单机
			if runtime, _ := s.stateService.GetOrCreateRuntime(pipeline.Id); nil != runtime {
				if runtime.IsRunning() || runtime.IsFault() || runtime.IsBatching() {
					running++
				}
			}
		}
	}
	counts["running"] = running

	RespData(c, counts)
}

func (s *DashboardAction) Analysis(c *gin.Context) {
	pipelines, _ := s.pipelineService.SelectList(vo.NewPipelineInfoParams())
	counts := make([]vo.AnalysisVO, 0, len(pipelines))

	for _, pipeline := range pipelines {
		v := vo.AnalysisVO{
			Name: pipeline.Name,
		}
		if service.IsClusterAndLeader() { //集群
			if state, _ := s.stateService.GetState(pipeline.Id); state != nil {
				v.InsertCount = state.InsertCount
				v.UpdateCount = state.UpdateCount
				v.DeleteCount = state.DeleteCount
			}
		} else { //单机
			if runtime, _ := s.stateService.GetOrCreateRuntime(pipeline.Id); nil != runtime {
				v.InsertCount = runtime.InsertCounter.Load()
				v.UpdateCount = runtime.UpdateCounter.Load()
				v.DeleteCount = runtime.DeleteCounter.Load()
			}
		}
		counts = append(counts, v)
	}

	RespData(c, counts)
}

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

	"go-mysql-transfer/service"
)

type ClusterAction struct {
}

func initClusterAction(r *gin.RouterGroup) {
	s := &ClusterAction{}
	r.GET("clusters/cluster", s.Cluster)
	r.GET("clusters/nodes", s.Nodes)
	r.DELETE("clusters/:addr/remove", s.Remove)
}

func (s *ClusterAction) Cluster(c *gin.Context) {
	RespData(c, gin.H{
		"isCluster": service.IsCluster(),
		"leader":    service.GetLeader(),
	})
}

func (s *ClusterAction) Nodes(c *gin.Context) {
	RespData(c, service.GetLeaderService().GetNodes())
}

func (s *ClusterAction) Remove(c *gin.Context) {
	addr := c.Param("addr")
	service.GetLeaderService().RemoveFollower(addr)
	RespOK(c)
}

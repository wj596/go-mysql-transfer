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

package service

import (
	"go-mysql-transfer/dao"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/snowflake"
)

type MachineService struct {
	dao dao.MachineDao
}

func (s *MachineService) initSnowflake() error {
	machineId, err := s.dao.GetId(GetCurrNode())
	if err != nil {
		return err
	}

	log.Infof("节点[%s], snowflake machineId[%d]", GetCurrNode(), machineId)
	snowflake.Initialize(machineId)

	return nil
}

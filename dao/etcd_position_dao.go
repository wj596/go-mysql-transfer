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

package dao

import (
	"sync"

	"github.com/siddontang/go-mysql/mysql"
)

type EtcdPositionDao struct {
	positions map[uint64]bool
	states    map[uint64]bool
	lock      sync.Mutex
}

func (s *EtcdPositionDao) Save(pipelineId uint64, position mysql.Position) error {
	return nil
}

func (s *EtcdPositionDao) Get(pipelineId uint64) mysql.Position {
	var entity mysql.Position
	return entity
}

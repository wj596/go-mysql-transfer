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

package mysql

import (
	"go-mysql-transfer/domain/po"
)

type MachineDaoImpl struct {
}

const (
	_selectMachineSql = "SELECT ID,NODE FROM T_MACHINE WHERE NODE = ?"
	_insertMachineSql = "INSERT INTO T_MACHINE(NODE) VALUES (?)"
)

func (s *MachineDaoImpl) GetMachineIndex(machineUrl string) (uint16, error) {
	var entity po.Machine
	exist, err := _orm.SQL(_selectMachineSql, machineUrl).Get(&entity)
	if err != nil {
		return 0, err
	}
	if exist {
		return uint16(entity.Id), nil
	}

	result, err := _orm.Exec(_insertMachineSql, machineUrl)
	if err != nil {
		return 0, err
	}

	var id int64
	if id, err = result.LastInsertId(); err != nil {
		return 0, err
	}

	return uint16(id), err
}

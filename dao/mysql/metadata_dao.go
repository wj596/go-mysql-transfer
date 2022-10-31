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

const (
	_selectMetadataVersionSql    = "SELECT VERSION FROM T_METADATA WHERE ID = ?"
	_selectAllMetadataVersionSql = "SELECT ID,VERSION FROM T_METADATA WHERE TYPE = ?"
	_selectMetadataSql           = "SELECT ID, TYPE, VERSION, DATA FROM T_METADATA WHERE ID = ?"
	_deleteMetadataSql           = "DELETE FROM T_METADATA WHERE ID = ?"
	_insertMetadataSql           = "INSERT INTO T_METADATA(ID, TYPE, VERSION, DATA) VALUES (?, ?, ?, ?)"
	_updateMetadataSql           = "UPDATE T_METADATA SET VERSION = VERSION+1, DATA = ? WHERE ID = ? AND VERSION = ?"
)

type MetadataDao struct {
}

func (s *MetadataDao) Insert(id uint64, metadataType int, marshaled []byte) error {
	_, err := _orm.Exec(_insertMetadataSql, id, metadataType, 1, marshaled)
	return err
}

func (s *MetadataDao) Delete(id uint64) error {
	_, err := _orm.Exec(_deleteMetadataSql, id)
	return err
}

func (s *MetadataDao) Update(id uint64, version int32, marshaled []byte) error {
	_, err := _orm.Exec(_updateMetadataSql, marshaled, id, version)
	return err
}

func (s *MetadataDao) GetDataVersion(id uint64) (int32, error) {
	var version int32
	_, err := _orm.SQL(_selectMetadataVersionSql, id).Get(&version)
	if err != nil {
		return 0, err
	}
	return version, nil
}

func (s *MetadataDao) SelectAllDataVersion(metadataType int) ([]*po.MetadataVersion, error) {
	var ls []*po.MetadataVersion
	err := _orm.SQL(_selectAllMetadataVersionSql, metadataType).Find(&ls)
	if err != nil {
		return nil, err
	}
	return ls, nil
}

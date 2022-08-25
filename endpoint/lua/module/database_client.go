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

package module

import (
	"database/sql"
	"fmt"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	"github.com/yuin/gopher-lua"

	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/luautils"
	"go-mysql-transfer/util/sqlutils"
)

var (
	_dbs     = make(map[string]*sql.DB)
	_dbsLock sync.RWMutex
)

func PreloadDatabaseClientModule(L *lua.LState) {
	L.PreloadModule("database", dbClientModuleLoader)
}

func dbClientModuleLoader(L *lua.LState) int {
	t := L.NewTable()
	L.SetFuncs(t, _dbClientApi)
	L.Push(t)
	return 1
}

var _dbClientApi = map[string]lua.LGFunction{
	"get":   get,
	"query": query,
}

func get(L *lua.LState) int {
	result := L.NewTable()
	schema := L.CheckString(1)
	statement := L.CheckString(2)
	dsFormat := luautils.LvToString(L.GetGlobal(constants.GlobalDataSourceName))
	dataSourceName := fmt.Sprintf(dsFormat, schema)

	if schema == "" {
		log.Warn("schema为空")
		L.Push(lua.LFalse)
		L.Push(lua.LString("schema为空"))
		L.Push(result)
		return 3
	}

	if statement == "" {
		log.Warn("sql statement为空")
		L.Push(lua.LFalse)
		L.Push(lua.LString("sql statement为空"))
		L.Push(result)
		return 3
	}

	var db *sql.DB
	var dbExist bool
	_dbsLock.RLocker().Lock()
	db, dbExist = _dbs[dataSourceName]
	_dbsLock.RLocker().Unlock()

	var err error
	if dbExist {
		log.Infof("获取数据库[%s]", schema)
		err = db.Ping()
		if err != nil { // ping 失败重建连接
			db.Close()
			db, err = createDatabaseConnection(dataSourceName, schema)
		}
	} else {
		db, err = createDatabaseConnection(dataSourceName, schema)
	}
	if err != nil {
		log.Warn(err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(result)
		return 3
	}

	log.Infof("数据库[%s],查询语句[%s]", schema, statement)
	var rows *sql.Rows
	rows, err = db.Query(statement)
	if err != nil {
		log.Warn(err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(result)
		return 3
	}
	defer rows.Close()

	columns, _ := rows.Columns()
	types, _ := rows.ColumnTypes()
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		rows.Scan(scanArgs...)
		for i, v := range values {
			if v != nil {
				vv := sqlutils.RawBytesToInterface(v, types[i].DatabaseTypeName())
				luautils.PaddingLuaTableWithValue(L, result, columns[i], vv)
			}
		}
	}

	L.Push(lua.LTrue)
	L.Push(lua.LString(""))
	L.Push(result)
	return 3
}

func query(L *lua.LState) int {
	result := L.NewTable()
	schema := L.CheckString(1)
	statement := L.CheckString(2)
	dsFormat := luautils.LvToString(L.GetGlobal(constants.GlobalDataSourceName))
	dataSourceName := fmt.Sprintf(dsFormat, schema)

	var db *sql.DB
	var dbExist bool
	_dbsLock.RLocker().Lock()
	db, dbExist = _dbs[dataSourceName]
	_dbsLock.RLocker().Unlock()

	var err error
	if dbExist {
		log.Infof("获取数据库[%s]", schema)
		err = db.Ping()
		if err != nil { // ping 失败重建连接
			db.Close()
			db, err = createDatabaseConnection(dataSourceName, schema)
		}
	} else {
		db, err = createDatabaseConnection(dataSourceName, schema)
	}
	if err != nil {
		log.Warn(err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(result)
		return 3
	}

	log.Infof("数据库[%s],查询语句[%s]", schema, statement)
	var rows *sql.Rows
	rows, err = db.Query(statement)
	if err != nil {
		log.Warn(err.Error())
		L.Push(lua.LFalse)
		L.Push(lua.LString(err.Error()))
		L.Push(result)
		return 3
	}
	defer rows.Close()

	columns, _ := rows.Columns()
	types, _ := rows.ColumnTypes()
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	var index int
	for rows.Next() {
		index++
		item := L.NewTable()
		rows.Scan(scanArgs...)
		for i, v := range values {
			vv := sqlutils.RawBytesToInterface(v, types[i].DatabaseTypeName())
			luautils.PaddingLuaTableWithValue(L, item, columns[i], vv)
		}
		L.SetTable(result, lua.LNumber(index), item)
	}

	log.Infof("查询数据条数[%d]", index)

	L.Push(lua.LTrue)
	L.Push(lua.LString(""))
	L.Push(result)
	return 3
}

func createDatabaseConnection(dataSourceName, schema string) (*sql.DB, error) {
	_dbsLock.Lock()
	defer _dbsLock.Unlock()

	db, err := sql.Open(constants.FlavorMysql, dataSourceName)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	_dbs[dataSourceName] = db
	log.Infof("创建数据库[%s]", schema)
	return db, nil
}

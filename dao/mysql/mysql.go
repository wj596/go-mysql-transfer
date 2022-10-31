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
	"github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	"xorm.io/core"

	"go-mysql-transfer/config"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/logagent"
)

const (
	_driverName  = "mysql"
	_tablePrefix = "t_" //表前缀
	_clusterName = "go-mysql-transfer"
	_electionSql = "INSERT IGNORE INTO T_ELECTION ( CLUSTER, LEADER, LAST_ACTIVE )" +
		" VALUES ( ?, ?, NOW() )" +
		" ON DUPLICATE KEY UPDATE" +
		" LEADER = IF ( TIMESTAMPDIFF(SECOND, LAST_ACTIVE, NOW()) > 2, VALUES ( LEADER ), LEADER )," +
		" LAST_ACTIVE = IF ( LEADER = VALUES ( LEADER ), VALUES ( LAST_ACTIVE ), LAST_ACTIVE )"
	_selectLeaderSql = "SELECT LEADER FROM T_ELECTION WHERE CLUSTER = ? AND TIMESTAMPDIFF(SECOND, LAST_ACTIVE, NOW()) <= ?"
	_selectDiffSql   = "SELECT TIMESTAMPDIFF(SECOND, LAST_ACTIVE, NOW()) FROM T_ELECTION WHERE CLUSTER = ?"
)

var (
	_orm *xorm.Engine
)

func Initialize(config *config.AppConfig) error {
	mysql.SetLogger(&logagent.MysqlLoggerAgent{})
	dataSourceName := config.GetClusterConfig().GetMySQLDataSourceName()
	orm, err := xorm.NewEngine(_driverName, dataSourceName)
	if err != nil {
		return err
	}

	if err = orm.Ping(); err != nil {
		return err
	}

	orm.SetLogger(logagent.NewXormLoggerAgent())
	orm.SetTableMapper(core.NewPrefixMapper(core.SnakeMapper{}, _tablePrefix))
	switch config.GetLoggerConfig().GetLevel() {
	case log.LevelInfo:
		orm.SetLogLevel(core.LOG_INFO)
		orm.ShowSQL(false)
	case log.LevelWarn:
		orm.SetLogLevel(core.LOG_WARNING)
		orm.ShowSQL(false)
	case log.LevelError:
		orm.SetLogLevel(core.LOG_ERR)
		orm.ShowSQL(false)
	default:
		orm.SetLogLevel(core.LOG_DEBUG)
		//	orm.ShowSQL(true)
	}

	_orm = orm
	return nil
}

func Close() {
	if _orm != nil {
		_orm.Close()
	}
}

func GetOrm() *xorm.Engine {
	return _orm
}

func UpdateElection(node string) (int64, error) {
	res, err := _orm.Exec(_electionSql, _clusterName, node)
	if err != nil {
		return 0, err
	}

	var aff int64
	aff, err = res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return aff, nil
}

func SelectLeader() (string, bool, error) {
	var leader string
	exist, err := _orm.SQL(_selectLeaderSql, _clusterName, constants.MySQLPreemptiveInterval).Get(&leader)
	if err != nil {
		return "", false, err
	}
	if !exist {
		return "", false, nil
	}
	return leader, true, nil
}

func SelectDiff() (int, error) {
	var diff int
	_, err := _orm.SQL(_selectDiffSql, _clusterName).Get(&diff)
	if err != nil {
		return 0, err
	}
	return diff, nil
}

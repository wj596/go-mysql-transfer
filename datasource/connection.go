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

package datasource

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/siddontang/go-mysql/canal"

	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/util/commons"
	"go-mysql-transfer/util/log"
)

func TestConnect(ds *po.SourceInfo) error {
	dataSourceName := commons.GetDataSourceName(ds.GetUsername(), ds.GetPassword(), ds.GetHost(), "mysql", ds.GetPort(), ds.GetCharset())
	db, err := sql.Open(ds.GetFlavor(), dataSourceName)
	if err != nil {
		return err
	}
	defer db.Close()

	log.Infof("测试数据库连接：user[%s]、host[%s]、port[%d]", ds.GetUsername(), ds.GetHost(), ds.GetPort())

	return db.Ping()
}

func CreateConnection(ds *po.SourceInfo) (*canal.Canal, error) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", ds.GetHost(), ds.GetPort())
	cfg.User = ds.GetUsername()
	cfg.Password = ds.GetPassword()
	cfg.Flavor = ds.GetFlavor()
	if ds.GetCharset() != "" {
		cfg.Charset = ds.GetCharset()
	}
	//if ds.GetSlaveID() != 0 {
	//cfg.ServerID = ds.GetSlaveID()
	//}
	cfg.Dump.DiscardErr = false
	cfg.Dump.ExecutionPath = ""

	canal, err := canal.NewCanal(cfg)
	if err != nil {
		return nil, err
	}

	return canal, nil
}

func CloseConnection(canal *canal.Canal) {
	if canal != nil {
		canal.Close()
	}
}

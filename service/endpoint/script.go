/*
 * Copyright 2020-2021 the original author(https://github.com/wj596)
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
package endpoint

import (
	"log"

	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/mysql"

	"go-mysql-transfer/global"
	"go-mysql-transfer/metrics"
	"go-mysql-transfer/model"
	"go-mysql-transfer/service/luaengine"
	"go-mysql-transfer/util/logs"
)

type ScriptEndpoint struct {
}

func newScriptEndpoint() *ScriptEndpoint {
	return &ScriptEndpoint{}
}

func (s *ScriptEndpoint) Connect() error {
	return nil
}

func (s *ScriptEndpoint) Ping() error {
	return nil
}

func (s *ScriptEndpoint) Consume(from mysql.Position, rows []*model.RowRequest) error {
	for _, row := range rows {
		rule, _ := global.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		metrics.UpdateActionNum(row.Action, row.RuleKey)
		kvm := rowMap(row, rule, true)
		err := luaengine.DoScript(kvm, row.Action, rule)
		if err != nil {
			log.Println("Lua 脚本执行失败!!! ,详情请参见日志")
			return errors.Errorf("Lua 脚本执行失败 : %s ", errors.ErrorStack(err))
		}
		kvm = nil
	}

	logs.Infof("处理完成 %d 条数据", len(rows))
	return nil
}

func (s *ScriptEndpoint) Stock(rows []*model.RowRequest) int64 {
	var counter int64
	for _, row := range rows {
		rule, _ := global.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		kvm := rowMap(row, rule, true)
		err := luaengine.DoScript(kvm, row.Action, rule)
		if err != nil {
			logs.Errorf("lua 脚本执行失败 : %s ", errors.ErrorStack(err))
			break
		}
		counter++
	}

	return counter
}

func (s *ScriptEndpoint) Close() {

}

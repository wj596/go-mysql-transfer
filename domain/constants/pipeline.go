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

package constants

// PipelineInfo 状态
const (
	PipelineInfoStatusEnable  = 0 //启用
	PipelineInfoStatusDisable = 9 //停用
)

const (
	PipelineRunStatusInitial  = 0 //未启动
	PipelineRunStatusFail     = 1 //启动失败
	PipelineRunStatusRunning  = 2 //运行中
	PipelineRunStatusFault    = 3 //故障
	PipelineRunStatusBatching = 4 //全量同步中
	PipelineRunStatusBatchEnd = 5 //全量同步结束
	PipelineRunStatusPanic    = 8 //崩溃
	PipelineRunStatusClose    = 9 //关闭
)

const (
	PipelineAlarmItemException = "1" //异常信息
	PipelineAlarmItemBatch     = "2" //全量同步报告
	PipelineAlarmItemStream    = "3" //每日同步报告
	PipelineAlarmItemCluster   = "4" //集群节点变动
)

const (
	RuleTypeLuaScript = 1 //脚本
)

const (
	ColumnNameFormatterLower = 0 //小写
	ColumnNameFormatterUpper = 1 //大写
	ColumnNameFormatterCamel = 2 //驼峰
)

const (
	DataEncoderJson       = 0 //json
	DataEncoderExpression = 1 //表达式
)

const (
	RedisStructureString    = 0
	RedisStructureHash      = 1
	RedisStructureList      = 2
	RedisStructureSet       = 3
	RedisStructureSortedSet = 4
)

const (
	RedisKeyBuilderColumnValue = 0 //使用列值
	RedisKeyBuilderExpression  = 1 //表达式
)

const (
	TableTypeSingle  = "0" //单个表
	TableTypeList    = "1" //列表
	TableTypePattern = "2" //正则表达式
)

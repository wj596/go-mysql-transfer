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

import "time"

const ApplicationName = "go-mysql-transfer"

const (
	EsIndexBuildTypeExtend     = "0" //使用已经存在的
	EsIndexBuildTypeAutoCreate = "1" //自动创建
)

const ( //Redis集群类型
	RedisGroupTypeSentinel = 1
	RedisGroupTypeCluster  = 2
)

// use by lua model
const (
	ExpireAction            = "expire"
	UpsertAction            = "upsert"
	TestAction            = "test"

	LuaGlobalVariableResult = "___RESULT___"

	LuaGlobalVariablePreRow = "___PRE_ROW___"
	LuaGlobalVariableRow    = "___ROW___"
	LuaGlobalVariableAction = "___ACTION___"
)

const (
	BatchBulkSize         = 100
	BatchCoroutines       = 3
	StreamBulkSize        = 100
	StreamFlushInterval   = 200 //毫秒
	PositionFlushInterval = time.Duration(3)   //秒
)

const (
	HttpTimeout = time.Duration(5) * time.Second
)

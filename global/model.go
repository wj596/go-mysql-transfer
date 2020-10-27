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
package global

import (
	"sync"

	"github.com/siddontang/go-mysql/schema"
)

type RowRequest struct {
	RuleKey string
	Action  string
	OldRow  []interface{}
	Row     []interface{}
}

type PosRequest struct {
	Name  string
	Pos   uint32
	Force bool
}

type RedisRespond struct {
	Action    string
	Structure string
	Key       string
	Field     string
	Score     float64
	OldVal    interface{}
	Val       interface{}
}

type MQRespond struct {
	Topic     string      `json:"-"`
	Action    string      `json:"action"`
	Date      interface{} `json:"date"`
	ByteArray []byte      `json:"-"`
}

type ESRespond struct {
	Index  string
	Id     string
	Action string
	Date   string
}

type MongoRespond struct {
	RuleKey    string
	Collection string
	Action     string
	Id         interface{}
	Table      map[string]interface{}
}

type Padding struct {
	WrapName string

	ColumnName     string
	ColumnIndex    int
	ColumnType     int
	ColumnMetadata *schema.TableColumn
}

var RedisRespondPool = sync.Pool{
	New: func() interface{} {
		return new(RedisRespond)
	},
}

var MQRespondPool = sync.Pool{
	New: func() interface{} {
		return new(MQRespond)
	},
}

var RowRequestPool = sync.Pool{
	New: func() interface{} {
		return new(RowRequest)
	},
}

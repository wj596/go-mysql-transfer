package global

import (
	"github.com/siddontang/go-mysql/schema"
	"sync"
)

type RowRequest struct {
	RuleKey string
	Action  string
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

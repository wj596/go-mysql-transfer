package global

import (
	"github.com/siddontang/go-mysql/schema"
	"sync"
)

type RowRequest struct {
	RuleKey string
	Action  int
	Row     []interface{}
}

type PosRequest struct {
	Name  string
	Pos   uint32
	Force bool
}

type RedisRespond struct {
	Key   string
	Field string
	Val   interface{}
}

type RocketmqRespond struct {
	Topic string
	Msg   []byte
}

type Padding struct {
	WrapName    string
	Column      *schema.TableColumn
	ColumnName  string
	ColumnIndex int
}

var RedisRespondPool = sync.Pool{
	New: func() interface{} {
		return new(RedisRespond)
	},
}

var RocketmqRespondPool = sync.Pool{
	New: func() interface{} {
		return new(RocketmqRespond)
	},
}

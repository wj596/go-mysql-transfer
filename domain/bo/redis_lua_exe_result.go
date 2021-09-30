package bo

import "sync"

type RedisLuaExeResult struct {
	Action       string
	Structure    int
	Key          string
	Field        string
	Score        float64
	Expiration   int64
	Value        interface{}
	CoveredValue interface{}
}

var redisLuaExeResultPool = sync.Pool{
	New: func() interface{} {
		return new(RedisLuaExeResult)
	},
}

func BorrowRedisLuaExeResult() *RedisLuaExeResult {
	return redisLuaExeResultPool.Get().(*RedisLuaExeResult)
}

func ReleaseRedisLuaExeResult(r *RedisLuaExeResult) {
	r.Action = ""
	r.Structure = 0
	r.Key = ""
	r.Field = ""
	r.Score = 0
	r.Expiration = 0
	r.Value = nil
	r.CoveredValue = nil
	redisLuaExeResultPool.Put(r)
}

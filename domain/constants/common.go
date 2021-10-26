package constants

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
	ActionExpire         = "expire"
	LuaGlobalVariableResult = "___RESULT___"

	LuaGlobalVariablePreRow = "___PRE_ROW___"
	LuaGlobalVariableRow    = "___ROW___"
	LuaGlobalVariableAction = "___ACTION___"
)

package constants

const (
	EsIndexBuildTypeExtend     = "0" //使用已经存在的
	EsIndexBuildTypeAutoCreate = "1" //自动创建
)

const ( //Redis集群类型
	RedisGroupTypeSentinel = 1
	RedisGroupTypeCluster  = 2
)

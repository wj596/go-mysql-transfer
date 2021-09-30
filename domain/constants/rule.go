package constants

const (
	TransformRuleTypeRule      = 0 //规则
	TransformRuleTypeLuaScript = 1 //脚本
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
	RedisStructureString    = 1
	RedisStructureHash      = 2
	RedisStructureList      = 3
	RedisStructureSet       = 4
	RedisStructureSortedSet = 5
)

const (
	RedisKeyBuilderColumnValue = 0 //使用列值
	RedisKeyBuilderExpression  = 1 //表达式
)

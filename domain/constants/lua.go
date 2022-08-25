package constants

import "github.com/yuin/gopher-lua"

const (
	SelectAction = "select"
	ExpireAction = "expire"
	UpsertAction = "upsert"
	TestAction   = "test"

	HandleFunctionName   = "handle"
	RowKey               = lua.LString("Row")
	PreRowKey            = lua.LString("PreRow")
	ActionKey            = lua.LString("Action")
	SchemaKey            = lua.LString("Schema")
	TableKey            = lua.LString("Table")
	EndpointKey          = "___ENDPOINT___"
	GlobalVariableResult = "___RESULT___"
	GlobalDataSourceName = "___DATA_SOURCE_NAME___"

	GlobalVariablePreRow = "___PRE_ROW___"
	GlobalVariableRow    = "___ROW___"
	GlobalVariableAction = "___ACTION___"
)

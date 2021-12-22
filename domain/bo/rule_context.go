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

package bo

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/json-iterator/go"
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/schema"
	"github.com/yuin/gopher-lua"
	"github.com/yuin/gopher-lua/parse"

	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/endpoint/luaengine"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/stringutils"
)

type Padding struct {
	WrapName    string
	ColumnName  string
	ColumnIndex int
	ColumnType  int
	EnumValues  []string
	SetValues   []string
}

// RuleContext Dumper Rule上下文
type RuleContext struct {
	pipelineId                uint64
	pipelineName              string
	mongodbCollectionFullName string
	endpointType              uint32
	rule                      *po.Rule
	tableInfo                 *schema.Table
	tableFullName             string
	paddingMap                map[string]*Padding
	pkPaddings                []*Padding
	rowSize                   int                //行宽度(字段数量)
	dataExpressionTmpl        *template.Template // 数据表达式模板
	redisKeyExpressionTmpl    *template.Template //redis KEY模板
	luaFunctionProto          *lua.FunctionProto // Lua 预编译
	lvm                       *lua.LState        // Lua 虚拟机
}

func CreateRuleContext(pipeline *po.PipelineInfo, rule *po.Rule, table *schema.Table, preLoadLuaVM bool) (*RuleContext, error) {
	// Lua脚本预编译
	var luaFunctionProto *lua.FunctionProto
	var lvm *lua.LState
	if constants.RuleTypeLuaScript == rule.Type {
		protoName := stringutils.UUID()
		reader := strings.NewReader(rule.GetLuaScript())
		chunk, err := parse.Parse(reader, protoName)
		if err != nil {
			return nil, err
		}
		luaFunctionProto, err = lua.Compile(chunk, protoName) //编译
		if err != nil {
			log.Error(err.Error())
			return nil, err
		}
		if preLoadLuaVM { //预加载LUA虚拟机
			L := luaengine.New(pipeline.EndpointType)
			funcFromProto := L.NewFunctionFromProto(luaFunctionProto)
			L.Push(funcFromProto)
			err = L.PCall(0, lua.MultRet, nil)
			if err != nil {
				L.Close()
				log.Error(err.Error())
				return nil, err
			}
			lvm = L
		}
	}

	tableFullName := strings.ToLower(rule.Schema + "." + rule.Table)
	ctx := &RuleContext{
		lvm:              lvm,
		luaFunctionProto: luaFunctionProto,
		pipelineId:       pipeline.Id,
		pipelineName:     pipeline.Name,
		rule:             rule,
		tableInfo:        table,
		tableFullName:    tableFullName,
		pkPaddings:       make([]*Padding, 0),
	}

	ctx.initPaddingMap()
	ctx.initExpression()
	ctx.initPkPaddings()

	if rule.GetAdditionalColumnValueMapping() != nil {
		ctx.rowSize = len(ctx.paddingMap) + len(rule.GetAdditionalColumnValueMapping())
	} else {
		ctx.rowSize = len(ctx.paddingMap)
	}

	if constants.EndpointTypeMongoDB == pipeline.GetEndpointType() {
		ctx.mongodbCollectionFullName = stringutils.Join(rule.MongodbDatabase, rule.MongodbCollection)
	}

	return ctx, nil
}

func (s *RuleContext) GetPrimaryKeyValue(request *RowEventRequest) interface{} {
	l := len(s.pkPaddings)
	if l == 0 {
		return nil
	}

	if l == 1 {
		p := s.pkPaddings[0]
		d := request.Data[p.ColumnIndex]
		v := s.convertColumnData(d, p)
		return v
	}

	var key string
	for _, p := range s.pkPaddings {
		d := request.Data[p.ColumnIndex]
		v := s.convertColumnData(d, p)
		key += stringutils.ToString(v)
	}
	return key
}

// GetRow 获取当前的行数据
func (s *RuleContext) GetRow(req *RowEventRequest) map[string]interface{} {
	kv := make(map[string]interface{}, s.rowSize)
	if s.rule.GetAdditionalColumnValueMapping() != nil {
		for k, v := range s.rule.GetAdditionalColumnValueMapping() {
			kv[k] = v
		}
	}
	for _, p := range s.paddingMap {
		kv[p.ColumnName] = s.convertColumnData(req.Data[p.ColumnIndex], p)
	}
	return kv
}

// GetWrappedRow 获取当前的行数据使用包装后的列名称
func (s *RuleContext) GetWrappedRow(req *RowEventRequest) map[string]interface{} {
	kv := make(map[string]interface{}, s.rowSize)

	if s.rule.GetAdditionalColumnValueMapping() != nil {
		for k, v := range s.rule.GetAdditionalColumnValueMapping() {
			kv[s.toWrapName(k)] = v
		}
	}

	for _, p := range s.paddingMap {
		kv[p.WrapName] = s.convertColumnData(req.Data[p.ColumnIndex], p)
	}
	return kv
}

//GetPreRow 获取变更之前的行数据
func (s *RuleContext) GetPreRow(req *RowEventRequest) map[string]interface{} {
	if nil == req.PreData {
		return nil
	}

	kv := make(map[string]interface{}, s.rowSize)

	if s.rule.GetAdditionalColumnValueMapping() != nil {
		for k, v := range s.rule.GetAdditionalColumnValueMapping() {
			kv[k] = v
		}
	}

	for _, p := range s.paddingMap {
		kv[p.ColumnName] = s.convertColumnData(req.PreData[p.ColumnIndex], p)
	}

	return kv
}

// GetWrappedPreRow 获取变更之前的行数据使用包装后的列名称
func (s *RuleContext) GetWrappedPreRow(req *RowEventRequest) map[string]interface{} {
	kv := make(map[string]interface{}, s.rowSize)

	if s.rule.GetAdditionalColumnValueMapping() != nil {
		for k, v := range s.rule.GetAdditionalColumnValueMapping() {
			kv[s.toWrapName(k)] = v
		}
	}

	for _, p := range s.paddingMap {
		kv[p.WrapName] = s.convertColumnData(req.PreData[p.ColumnIndex], p)
	}
	return kv
}

func (s *RuleContext) EncodeValue(req *RowEventRequest) (string, error) {
	kv := make(map[string]interface{}, s.rowSize)

	//Json
	if constants.DataEncoderJson == s.rule.DataEncoder {
		if s.rule.GetAdditionalColumnValueMapping() != nil {
			for k, v := range s.rule.GetAdditionalColumnValueMapping() {
				kv[s.toWrapName(k)] = v
			}
		}
		for _, p := range s.paddingMap {
			kv[p.WrapName] = s.convertColumnData(req.Data[p.ColumnIndex], p)
		}

		data, err := jsoniter.Marshal(kv)
		if err != nil {
			return "", err
		}
		return string(data), nil
	}

	//Expression
	if constants.DataEncoderExpression == s.rule.DataEncoder {
		if s.rule.GetAdditionalColumnValueMapping() != nil {
			for k, v := range s.rule.GetAdditionalColumnValueMapping() {
				kv[k] = v
			}
		}
		for _, p := range s.paddingMap {
			kv[p.ColumnName] = s.convertColumnData(req.Data[p.ColumnIndex], p)
		}

		var tmplBytes bytes.Buffer
		err := s.dataExpressionTmpl.Execute(&tmplBytes, kv)
		if err != nil {
			return "", err
		}
		return tmplBytes.String(), nil
	}

	return "", errors.New("不支持的数据编码类型")
}

func (s *RuleContext) EncodePreValue(req *RowEventRequest) (string, error) {
	kv := make(map[string]interface{}, s.rowSize)

	//Json
	if constants.DataEncoderJson == s.rule.DataEncoder {
		if s.rule.GetAdditionalColumnValueMapping() != nil {
			for k, v := range s.rule.GetAdditionalColumnValueMapping() {
				kv[s.toWrapName(k)] = v
			}
		}
		for _, p := range s.paddingMap {
			kv[p.WrapName] = s.convertColumnData(req.PreData[p.ColumnIndex], p)
		}

		data, err := jsoniter.Marshal(kv)
		if err != nil {
			return "", err
		}
		return string(data), nil
	}

	//Expression
	if constants.DataEncoderExpression == s.rule.DataEncoder {
		if s.rule.GetAdditionalColumnValueMapping() != nil {
			for k, v := range s.rule.GetAdditionalColumnValueMapping() {
				kv[k] = v
			}
		}
		for _, p := range s.paddingMap {
			kv[p.ColumnName] = s.convertColumnData(req.PreData[p.ColumnIndex], p)
		}

		var tmplBytes bytes.Buffer
		err := s.dataExpressionTmpl.Execute(&tmplBytes, kv)
		if err != nil {
			return "", err
		}
		return tmplBytes.String(), nil
	}

	return "", errors.New("不支持的数据编码类型")
}

func (s *RuleContext) GetRule() *po.Rule {
	return s.rule
}

func (s *RuleContext) GetTableColumnCount() int {
	return len(s.tableInfo.Columns)
}

func (s *RuleContext) GetTableInfo() *schema.Table {
	return s.tableInfo
}

func (s *RuleContext) GetTableFullName() string {
	return s.tableFullName
}

func (s *RuleContext) GetTableColumn(column string) (*schema.TableColumn, int) {
	for index, c := range s.tableInfo.Columns {
		if strings.ToLower(c.Name) == strings.ToLower(column) {
			return &c, index
		}
	}
	return nil, -1
}

func (s *RuleContext) GetTableColumnIndex(column string) int {
	for index, _ := range s.tableInfo.Columns {
		if strings.ToLower(column) == strings.ToLower(column) {
			return index
		}
	}
	return -1
}

func (s *RuleContext) GetPadding(column string) (*Padding, bool) {
	p, ok := s.paddingMap[strings.ToLower(column)]
	return p, ok
}

func (s *RuleContext) IsLuaEnable() bool {
	return s.rule.Type == constants.RuleTypeLuaScript
}

func (s *RuleContext) GetLuaFunctionProto() *lua.FunctionProto {
	return s.luaFunctionProto
}

func (s *RuleContext) IsReservePreData() bool {
	return s.rule.ReserveCoveredData
}

func (s *RuleContext) GetRedisKeyExpressionTmpl() *template.Template {
	return s.redisKeyExpressionTmpl
}

func (s *RuleContext) GetPipelineName() string {
	return s.pipelineName
}

func (s *RuleContext) GetMongodbCollectionFullName() string {
	return s.mongodbCollectionFullName
}

// GetLuaVM 获取LUA虚拟机
func (s *RuleContext) GetLuaVM() *lua.LState {
	return s.lvm
}

// CloseLuaVM 关闭LUA虚拟机
func (s *RuleContext) CloseLuaVM() {
	if s.IsLuaEnable() && s.lvm != nil {
		s.lvm.Close()
	}
}

func (s *RuleContext) initExpression() {
	if constants.DataEncoderExpression == s.rule.DataEncoder {
		tmpl, _ := template.New(s.tableFullName).Parse(s.rule.DataExpression)
		s.dataExpressionTmpl = tmpl
	}
	if s.rule.RedisKeyExpression != "" {
		tmpl, _ := template.New(s.tableFullName).Parse(s.rule.RedisKeyExpression)
		s.redisKeyExpressionTmpl = tmpl
	}
}

func (s *RuleContext) initPaddingMap() {
	paddingMap := make(map[string]*Padding)
	for index, column := range s.tableInfo.Columns {
		include := true
		for _, exclude := range s.rule.GetExcludeColumnList() {
			if strings.ToLower(column.Name) == strings.ToLower(exclude) {
				include = false
			}
		}
		if include {
			padding := &Padding{
				WrapName:    s.toWrapName(column.Name),
				ColumnIndex: index,
				ColumnName:  column.Name,
				ColumnType:  column.Type,
				EnumValues:  column.EnumValues,
				SetValues:   column.SetValues,
			}
			paddingMap[strings.ToLower(column.Name)] = padding
		}
	}
	s.paddingMap = paddingMap
}

func (s *RuleContext) initPkPaddings() {
	if len(s.tableInfo.PKColumns) > 0 {
		for _, index := range s.tableInfo.PKColumns {
			column := s.tableInfo.Columns[index]
			padding := &Padding{
				WrapName:    s.toWrapName(column.Name),
				ColumnIndex: index,
				ColumnName:  column.Name,
				ColumnType:  column.Type,
				EnumValues:  column.EnumValues,
				SetValues:   column.SetValues,
			}
			s.pkPaddings = append(s.pkPaddings, padding)
		}
	}
}

func (s *RuleContext) toWrapName(column string) string {
	if s.rule.GetType() == constants.RuleTypeLuaScript {
		return column
	}

	if s.rule.GetColumnNameMapping() != nil {
		for k, v := range s.rule.GetColumnNameMapping() {
			if strings.ToLower(k) == strings.ToLower(column) {
				return v
			}
		}
	}

	if constants.ColumnNameFormatterCamel == s.rule.ColumnNameFormatter {
		return stringutils.Case2Camel(strings.ToLower(column))
	}
	if constants.ColumnNameFormatterLower == s.rule.ColumnNameFormatter {
		return strings.ToLower(column)
	}
	if constants.ColumnNameFormatterUpper == s.rule.ColumnNameFormatter {
		return strings.ToUpper(column)
	}
	return column
}

func (s *RuleContext) convertColumnData(value interface{}, padding *Padding) interface{} {
	if value == nil {
		return nil
	}

	switch padding.ColumnType {
	case schema.TYPE_ENUM:
		switch value := value.(type) {
		case int64:
			eNum := value - 1
			if eNum < 0 || eNum >= int64(len(padding.EnumValues)) {
				log.Warnf("invalid binlog enum index %d, for enum %v", eNum, padding.EnumValues)
				return ""
			}
			return padding.EnumValues[eNum]
		case string:
			return value
		case []byte:
			return string(value)
		}
	case schema.TYPE_SET:
		switch value := value.(type) {
		case int64:
			bitmask := value
			sets := make([]string, 0, len(padding.SetValues))
			for i, s := range padding.SetValues {
				if bitmask&int64(1<<uint(i)) > 0 {
					sets = append(sets, s)
				}
			}
			return strings.Join(sets, ",")
		}
	case schema.TYPE_BIT:
		switch value := value.(type) {
		case string:
			if value == "\x01" {
				return int64(1)
			}
			return int64(0)
		}
	case schema.TYPE_STRING:
		switch value := value.(type) {
		case []byte:
			return string(value[:])
		}
	case schema.TYPE_JSON:
		var f interface{}
		var err error
		switch v := value.(type) {
		case string:
			err = json.Unmarshal([]byte(v), &f)
		case []byte:
			err = json.Unmarshal(v, &f)
		}
		if err == nil && f != nil {
			return f
		}
	case schema.TYPE_DATETIME, schema.TYPE_TIMESTAMP:
		var vv string
		switch v := value.(type) {
		case string:
			vv = v
		case []byte:
			vv = string(v)
		}
		if s.rule.DatetimeFormatter != "" {
			vt, err := time.Parse(mysql.TimeFormat, vv)
			if err != nil || vt.IsZero() { // failed to parse date or zero date
				return nil
			}
			return vt.Format(s.rule.DatetimeFormatter)
		}
		return vv
	case schema.TYPE_DATE:
		var vv string
		switch v := value.(type) {
		case string:
			vv = v
		case []byte:
			vv = string(v)
		}
		if s.rule.DateFormatter != "" {
			vt, err := time.Parse("2006-01-02", vv)
			if err != nil || vt.IsZero() { // failed to parse date or zero date
				return nil
			}
			return vt.Format(s.rule.DateFormatter)
		}
		return vv
	case schema.TYPE_NUMBER:
		switch v := value.(type) {
		case string:
			vv, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				log.Errorf("ConvertColumnData error[%s]", err.Error())
				return nil
			}
			return vv
		case []byte:
			str := string(v)
			vv, err := strconv.ParseInt(str, 10, 64)
			if err != nil {
				log.Errorf("ConvertColumnData error[%s]", err.Error())
				return nil
			}
			return vv
		}
	case schema.TYPE_DECIMAL, schema.TYPE_FLOAT:
		switch v := value.(type) {
		case string:
			vv, err := strconv.ParseFloat(v, 64)
			if err != nil {
				log.Errorf("ConvertColumnData error[%s]", err.Error())
				return nil
			}
			return vv
		case []byte:
			str := string(v)
			vv, err := strconv.ParseFloat(str, 64)
			if err != nil {
				log.Errorf("ConvertColumnData error[%s]", err.Error())
				return nil
			}
			return vv
		}
	}

	return value
}

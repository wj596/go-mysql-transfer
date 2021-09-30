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
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/stringutils"
)

var RuntimeRules = make(map[string]*RuntimeRule)

type Padding struct {
	WrapName    string
	ColumnName  string
	ColumnIndex int
	ColumnType  int
	EnumValues  []string
	SetValues   []string
	//ColumnMetadata *schema.TableColumn
}

type RuntimeRule struct {
	def                    *po.TransformRule
	tableInfo              *schema.Table
	paddings               map[string]*Padding
	rowMapLength           int
	dataExpressionTmpl     *template.Template
	redisKeyExpressionTmpl *template.Template
	luaFunctionProto       *lua.FunctionProto
}

func NewRuntimeRule(r *po.TransformRule, t *schema.Table) *RuntimeRule {
	rr := &RuntimeRule{
		def:       r,
		tableInfo: t,
	}

	rr.initPaddings()
	rr.initExpression()

	if r.GetAdditionalColumnValueMapping() != nil {
		rr.rowMapLength = len(rr.paddings) + len(r.GetAdditionalColumnValueMapping())
	} else {
		rr.rowMapLength = len(rr.paddings)
	}

	if constants.TransformRuleTypeLuaScript == r.Type {
		protoName := stringutils.UUID()
		reader := strings.NewReader(r.GetLuaScript())
		chunk, _ := parse.Parse(reader, protoName)
		proto, _ := lua.Compile(chunk, protoName)
		rr.luaFunctionProto = proto
	}

	return rr
}

func (s *RuntimeRule) GetRawRowMap(req *RowEventRequest) map[string]interface{} {
	kv := make(map[string]interface{}, s.rowMapLength)

	if s.def.GetAdditionalColumnValueMapping() != nil {
		for k, v := range s.def.GetAdditionalColumnValueMapping() {
			kv[k] = v
		}
	}

	for _, p := range s.paddings {
		kv[p.ColumnName] = s.convertColumnData(req.Row[p.ColumnIndex], p)
	}
	return kv
}

func (s *RuntimeRule) GetWrapRowMap(req *RowEventRequest) map[string]interface{} {
	kv := make(map[string]interface{}, s.rowMapLength)

	if s.def.GetAdditionalColumnValueMapping() != nil {
		for k, v := range s.def.GetAdditionalColumnValueMapping() {
			kv[s.toWrapName(k)] = v
		}
	}

	for _, p := range s.paddings {
		kv[p.WrapName] = s.convertColumnData(req.Row[p.ColumnIndex], p)
	}
	return kv
}

func (s *RuntimeRule) EncodeValue(req *RowEventRequest) (string, error) {
	kv := make(map[string]interface{}, s.rowMapLength)

	if constants.DataEncoderJson == s.def.DataEncoder {
		if s.def.GetAdditionalColumnValueMapping() != nil {
			for k, v := range s.def.GetAdditionalColumnValueMapping() {
				kv[s.toWrapName(k)] = v
			}
		}
		for _, p := range s.paddings {
			kv[p.WrapName] = s.convertColumnData(req.Row[p.ColumnIndex], p)
		}

		data, err := jsoniter.Marshal(kv)
		if err != nil {
			return "", err
		}
		return string(data), nil
	}

	if constants.DataEncoderExpression == s.def.DataEncoder {
		if s.def.GetAdditionalColumnValueMapping() != nil {
			for k, v := range s.def.GetAdditionalColumnValueMapping() {
				kv[k] = v
			}
		}
		for _, p := range s.paddings {
			kv[p.ColumnName] = s.convertColumnData(req.Row[p.ColumnIndex], p)
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

func (s *RuntimeRule) GetRawCoveredMap(req *RowEventRequest) map[string]interface{} {
	if nil == req.Covered {
		return nil
	}

	kv := make(map[string]interface{}, s.rowMapLength)

	if s.def.GetAdditionalColumnValueMapping() != nil {
		for k, v := range s.def.GetAdditionalColumnValueMapping() {
			kv[k] = v
		}
	}

	for _, p := range s.paddings {
		kv[p.ColumnName] = s.convertColumnData(req.Covered[p.ColumnIndex], p)
	}
	return kv
}

func (s *RuntimeRule) GetWrapCoveredMap(req *RowEventRequest) map[string]interface{} {
	kv := make(map[string]interface{}, s.rowMapLength)

	if s.def.GetAdditionalColumnValueMapping() != nil {
		for k, v := range s.def.GetAdditionalColumnValueMapping() {
			kv[s.toWrapName(k)] = v
		}
	}

	for _, p := range s.paddings {
		kv[p.WrapName] = s.convertColumnData(req.Covered[p.ColumnIndex], p)
	}
	return kv
}

func (s *RuntimeRule) EncodeCoveredValue(req *RowEventRequest) (string, error) {
	kv := make(map[string]interface{}, s.rowMapLength)

	if constants.DataEncoderJson == s.def.DataEncoder {
		if s.def.GetAdditionalColumnValueMapping() != nil {
			for k, v := range s.def.GetAdditionalColumnValueMapping() {
				kv[s.toWrapName(k)] = v
			}
		}
		for _, p := range s.paddings {
			kv[p.WrapName] = s.convertColumnData(req.Covered[p.ColumnIndex], p)
		}

		data, err := jsoniter.Marshal(kv)
		if err != nil {
			return "", err
		}
		return string(data), nil
	}

	if constants.DataEncoderExpression == s.def.DataEncoder {
		if s.def.GetAdditionalColumnValueMapping() != nil {
			for k, v := range s.def.GetAdditionalColumnValueMapping() {
				kv[k] = v
			}
		}
		for _, p := range s.paddings {
			kv[p.ColumnName] = s.convertColumnData(req.Covered[p.ColumnIndex], p)
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

func (s *RuntimeRule) GetDef() *po.TransformRule {
	return s.def
}

func (s *RuntimeRule) GetTableColumnCount() int {
	return len(s.tableInfo.Columns)
}

func (s *RuntimeRule) GetTableColumn(column string) (*schema.TableColumn, int) {
	for index, c := range s.tableInfo.Columns {
		if strings.ToLower(c.Name) == strings.ToLower(column) {
			return &c, index
		}
	}
	return nil, -1
}

func (s *RuntimeRule) GetTableColumnIndex(column string) int {
	for index, _ := range s.tableInfo.Columns {
		if strings.ToLower(column) == strings.ToLower(column) {
			return index
		}
	}
	return -1
}

func (s *RuntimeRule) GetPadding(column string) (*Padding, bool) {
	p, ok := s.paddings[strings.ToLower(column)]
	return p, ok
}

func (s *RuntimeRule) IsLuaScript() bool {
	return s.def.Type == constants.TransformRuleTypeLuaScript
}

func (s *RuntimeRule) GetLuaFunctionProto() *lua.FunctionProto {
	return s.luaFunctionProto
}

func (s *RuntimeRule) IsReserveCoveredData() bool {
	return s.def.ReserveCoveredData
}

func (s *RuntimeRule) GetRedisKeyExpressionTmpl() *template.Template {
	return s.redisKeyExpressionTmpl
}

func (s *RuntimeRule) initExpression() {
	if constants.DataEncoderExpression == s.def.DataEncoder {
		tmpl, _ := template.New(strconv.FormatUint(s.def.Id, 10)).Parse(s.def.DataExpression)
		s.dataExpressionTmpl = tmpl
	}
	if s.def.RedisKeyExpression != "" {
		tmpl, _ := template.New(strconv.FormatUint(s.def.Id, 10)).Parse(s.def.RedisKeyExpression)
		s.redisKeyExpressionTmpl = tmpl
	}
}

func (s *RuntimeRule) initPaddings() {
	paddings := make(map[string]*Padding)
	for index, column := range s.tableInfo.Columns {
		include := true
		for _, exclude := range s.def.GetExcludeColumnList() {
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
			paddings[strings.ToLower(column.Name)] = padding
		}
	}
	s.paddings = paddings
}

func (s *RuntimeRule) toWrapName(column string) string {
	if s.def.GetType() == constants.TransformRuleTypeLuaScript {
		return column
	}

	if s.def.GetColumnNameMapping() != nil {
		for k, v := range s.def.GetColumnNameMapping() {
			if strings.ToLower(k) == strings.ToLower(column) {
				return v
			}
		}
	}

	if constants.ColumnNameFormatterCamel == s.def.ColumnNameFormatter {
		return stringutils.Case2Camel(strings.ToLower(column))
	}
	if constants.ColumnNameFormatterLower == s.def.ColumnNameFormatter {
		return strings.ToLower(column)
	}
	if constants.ColumnNameFormatterUpper == s.def.ColumnNameFormatter {
		return strings.ToUpper(column)
	}
	return column
}

func (s *RuntimeRule) convertColumnData(value interface{}, padding *Padding) interface{} {
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
		if s.def.DatetimeFormatter != "" {
			vt, err := time.Parse(mysql.TimeFormat, vv)
			if err != nil || vt.IsZero() { // failed to parse date or zero date
				return nil
			}
			return vt.Format(s.def.DatetimeFormatter)
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
		if s.def.DateFormatter != "" {
			vt, err := time.Parse("2006-01-02", vv)
			if err != nil || vt.IsZero() { // failed to parse date or zero date
				return nil
			}
			return vt.Format(s.def.DateFormatter)
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

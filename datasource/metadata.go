package datasource

import (
	"fmt"
	"github.com/siddontang/go-mysql/canal"
	"go-mysql-transfer/model/vo"
	"strings"
)

func SelectSchemaNameList(cc *canal.Canal) ([]string, error) {
	sql := "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA"
	res, err := cc.Execute(sql)
	if err != nil {
		return nil, err
	}

	list := make([]string, 0)
	for i := 0; i < res.Resultset.RowNumber(); i++ {
		schemaName, err := res.GetString(i, 0)
		if err != nil {
			return nil, err
		}
		list = append(list, schemaName)
	}
	return list, nil
}

func SelectTableNameList(cc *canal.Canal, schemaName string) ([]string, error) {
	sql := "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' "
	res, err := cc.Execute(fmt.Sprintf(sql, schemaName))
	if err != nil {
		return nil, err
	}

	list := make([]string, 0)
	for i := 0; i < res.Resultset.RowNumber(); i++ {
		tableName, err := res.GetString(i, 0)
		if err != nil {
			return nil, err
		}
		list = append(list, tableName)
	}
	return list, nil
}

func SelectTableInfo(cc *canal.Canal, schemaName, tableName string) (*vo.TableInfo, error) {
	raw, err := cc.GetTable(schemaName, tableName)
	if err != nil {
		return nil, err
	}

	result := vo.TableInfo{
		Schema: raw.Schema,
		Name:   raw.Name,
	}

	columns := make([]*vo.TableColumnInfo, len(raw.Columns))
	for i, c := range raw.Columns {
		columns[i] = &vo.TableColumnInfo{
			Name:       strings.ToLower(c.Name),
			Type:       c.Type,
			Collation:  c.Collation,
			RawType:    c.RawType,
			IsAuto:     c.IsAuto,
			IsUnsigned: c.IsUnsigned,
			IsVirtual:  c.IsVirtual,
			EnumValues: c.EnumValues,
			SetValues:  c.SetValues,
			FixedSize:  c.FixedSize,
			MaxSize:    c.MaxSize,
		}
	}
	pks := make([]string, len(raw.PKColumns))
	for i, c := range raw.PKColumns {
		temp := raw.Columns[c]
		pks[i] = strings.ToLower(temp.Name)
	}

	result.PrimaryKeys = pks
	result.Columns = columns

	return &result, err
}

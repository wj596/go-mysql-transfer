package datasource

import (
	"fmt"
	"strings"

	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/schema"

	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/po"
)

func SelectSchemaNameList(ds *po.SourceInfo) ([]string, error) {
	con, err := CreateConnection(ds)
	if err != nil {
		return nil, err
	}
	defer con.Close()

	sql := "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA"
	var res *mysql.Result
	res, err = con.Execute(sql)
	if err != nil {
		return nil, err
	}
	defer res.Close()

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

func SelectTableNameList(ds *po.SourceInfo, schemaName string) ([]string, error) {
	con, err := CreateConnection(ds)
	if err != nil {
		return nil, err
	}
	defer con.Close()

	sql := "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' "
	var res *mysql.Result
	res, err = con.Execute(fmt.Sprintf(sql, schemaName))
	if err != nil {
		return nil, err
	}
	defer res.Close()

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

func SelectTableInfo(ds *po.SourceInfo, schemaName, tableName string) (*bo.TableInfo, error) {
	con, err := CreateConnection(ds)
	if err != nil {
		return nil, err
	}
	defer con.Close()

	var mata *schema.Table
	mata, err = con.GetTable(schemaName, tableName)
	if err != nil {
		return nil, err
	}

	result := bo.TableInfo{
		Schema: mata.Schema,
		Name:   mata.Name,
	}

	columns := make([]*bo.TableColumnInfo, len(mata.Columns))
	for i, c := range mata.Columns {
		columns[i] = &bo.TableColumnInfo{
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
	pks := make([]string, len(mata.PKColumns))
	for i, c := range mata.PKColumns {
		temp := mata.Columns[c]
		pks[i] = strings.ToLower(temp.Name)
	}

	result.PrimaryKeys = pks
	result.Columns = columns

	return &result, err
}

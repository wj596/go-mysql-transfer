package datasource

import (
	"fmt"
	"strings"

	"github.com/siddontang/go-mysql/canal"

	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/po"
)

func SelectSchemaNameList(ds *po.SourceInfo) ([]string, error) {
	cc, err := CreateCanal(ds)
	if err != nil {
		return nil, err
	}
	defer cc.Close()

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

func SelectTableNameList(ds *po.SourceInfo, schemaName string) ([]string, error) {
	cc, err := CreateCanal(ds)
	if err != nil {
		return nil, err
	}
	defer cc.Close()

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

func SelectTableInfo(ds *po.SourceInfo, schemaName, tableName string) (*bo.TableInfo, error) {
	cc, err := CreateCanal(ds)
	if err != nil {
		return nil, err
	}
	defer cc.Close()

	raw, err := cc.GetTable(schemaName, tableName)
	if err != nil {
		return nil, err
	}

	result := bo.TableInfo{
		Schema: raw.Schema,
		Name:   raw.Name,
	}

	columns := make([]*bo.TableColumnInfo, len(raw.Columns))
	for i, c := range raw.Columns {
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
	pks := make([]string, len(raw.PKColumns))
	for i, c := range raw.PKColumns {
		temp := raw.Columns[c]
		pks[i] = strings.ToLower(temp.Name)
	}

	result.PrimaryKeys = pks
	result.Columns = columns

	return &result, err
}

func CreateCanal(ds *po.SourceInfo) (*canal.Canal, error) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", ds.GetHost(), ds.GetPort())
	cfg.User = ds.GetUsername()
	cfg.Password = ds.GetPassword()
	cfg.Flavor = ds.GetFlavor()
	if ds.GetCharset() != "" {
		cfg.Charset = ds.GetCharset()
	}
	if ds.GetSlaveID() != 0 {
		cfg.ServerID = ds.GetSlaveID()
	}
	cfg.Dump.DiscardErr = false
	cfg.Dump.ExecutionPath = ""

	canal, err := canal.NewCanal(cfg)
	if err != nil {
		return nil, err
	}

	return canal, nil
}

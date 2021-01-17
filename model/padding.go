package model

import "github.com/siddontang/go-mysql/schema"

type Padding struct {
	WrapName string

	ColumnName     string
	ColumnIndex    int
	ColumnType     int
	ColumnMetadata *schema.TableColumn
}

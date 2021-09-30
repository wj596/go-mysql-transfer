package bo

type TableInfo struct {
	Schema      string             `json:"schema"`
	Name        string             `json:"name"`
	Columns     []*TableColumnInfo `json:"columns"`
	PrimaryKeys []string           `json:"primaryKeys"`
	Comments    string             `json:"comments"`
}

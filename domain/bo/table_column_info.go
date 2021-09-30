package bo

type TableColumnInfo struct {
	Name       string   `json:"name"`
	Type       int      `json:"type"`
	Collation  string   `json:"collation"`
	RawType    string   `json:"rawType"`
	IsAuto     bool     `json:"isAuto"`
	IsUnsigned bool     `json:"isUnsigned"`
	IsVirtual  bool     `json:"isVirtual"`
	EnumValues []string `json:"enumValues"`
	SetValues  []string `json:"setValues"`
	FixedSize  uint     `json:"fixedSize"`
	MaxSize    uint     `json:"maxSize"`
}

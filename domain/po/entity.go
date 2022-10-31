package po

type Machine struct {
	Id   int `json:"id,string" xorm:"pk"`
	Node string
}

type PositionEntity struct {
	Id   uint64 `json:"id,string" xorm:"pk"`
	Name string
	Pos  uint32
}

type Metadata struct {
	Id      uint64 `json:"id,string" xorm:"pk"`
	Type    int
	Version int32
	Data    []byte
}

type StateEntity struct {
	Id          uint64 `json:"id,string" xorm:"pk"`
	Status      uint32
	InsertCount uint64
	UpdateCount uint64
	DeleteCount uint64
	Node        string
	StartTime   string
	UpdateTime  int64
}

type MetadataVersion struct {
	Id      uint64
	Version int32
}

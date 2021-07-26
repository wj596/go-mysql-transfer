package model

// Replog = Replicated Log
type Replog struct {
	Id        uint64
	Type      uint8
	Target    uint8
	TargetId  uint8
	timestamp int64
	Context   []byte
}

package model

const (
	PipelineInfoNormal  = 1
	PipelineInfoDisable = 2
)

// PipelineInfo 管道
type PipelineInfo struct {
	Id         uint64
	Name       string
	SourceId   uint64
	TargetId   uint64
	Status     uint8
	CreateTime int64
	UpdateTime int64
}

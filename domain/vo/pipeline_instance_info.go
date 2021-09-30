package vo

// PipelineInstanceInfoVO '通道实例'值对象
type PipelineInstanceInfoVO struct {
	StartTime string `json:"startTime"`
	PosName   string `json:"posName"`
	PosIndex  uint32 `json:"posIndex"`
}

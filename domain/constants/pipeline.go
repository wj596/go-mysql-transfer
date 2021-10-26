package constants

// PipelineInfo 状态
const (
	PipelineInfoStatusEnable  = 0 //启用
	PipelineInfoStatusDisable = 9 //停用
)

const (
	PipelineRunStatusRunning  = 1 //运行中
	PipelineRunStatusPause    = 2 //暂停
	PipelineRunStatusFault    = 3 //故障
	PipelineRunStatusBatching = 4 //全量同步中
	PipelineRunStatusBatchEnd = 5 //全量同步结束
	PipelineRunStatusCease    = 9 //停止
)

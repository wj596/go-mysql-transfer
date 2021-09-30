package constants

// PipelineInfo 状态
const (
	PipelineInfoStatusInitialized = 0 //未启动
	PipelineInfoStatusStarted     = 1 //已启动
	PipelineInfoStatusRunning     = 2 //运行中
	PipelineInfoStatusPause       = 3 //暂停
	PipelineInfoStatusFullSync    = 5 //全量同步
	PipelineInfoStatusFault       = 9 //异常
)

const (
	FlushBulkInterval = 200
	FlushBulkSize     = 100
)

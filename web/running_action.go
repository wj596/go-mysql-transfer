package web

import (
	"github.com/gin-gonic/gin"
	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/vo"
	"go-mysql-transfer/service"
	"go-mysql-transfer/util/log"
)

type RunningAction struct {
	pipelineService *service.PipelineInfoService
}

func initRunningAction(r *gin.Engine) {
	s := &RunningAction{
		pipelineService: service.GetPipelineInfoService(),
	}

	r.GET("runnings", s.Select)
	//r.PUT("runtimes/:id/start-stream", s.StartStream)
	//r.PUT("runtimes/:id/stop-stream", s.StopStream)
	//r.PUT("runtimes/:id/start-batch", s.StartBatch)
	//r.PUT("runtimes/:id/position", s.SetPosition)
}

func (s *RunningAction) Select(c *gin.Context) {
	params := &vo.PipelineInfoParams{
		Name: c.Query("name"),
	}
	rets := make([]map[string]interface{}, 0)
	items, err := s.pipelineService.SelectList(params)
	if nil != err {
		log.Errorf("获取数据失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}

	for _, pipeline := range items {
		if pipeline.Status == constants.PipelineInfoStatusDisable {
			continue
		}

		ret := make(map[string]interface{}, 0)
		ret["pipelineId"] = pipeline.Id
		ret["pipelineName"] = pipeline.Name
		ret["status"] = 0
		ret["startTime"] = ""
		ret["batchEndTime"] = ""
		ret["message"] = ""
		ret["insertCount"] = 0
		ret["updateCount"] = 0
		ret["deleteCount"] = 0

		runtime, exist := bo.GetPipelineRunState(pipeline.Id)
		if exist {
			ret["status"] = runtime.GetStatus()
			ret["message"] = runtime.GetMessage()
			ret["startTime"] = runtime.GetStartTime()
			ret["batchEndTime"] = runtime.GetBatchEndTime()
			ret["positionName"] = runtime.GetPositionName()
			ret["positionIndex"] = runtime.GetPositionIndex()
			ret["insertCount"] = runtime.GetInsertCount()
			ret["updateCount"] = runtime.GetUpdateCount()
			ret["deleteCount"] = runtime.GetDeleteCount()

			//switch runtime.GetStatus() {
			//case constants.PipelineRunStatusRunning:
			//	ret["startTime"] = runtime.GetStartTime()
			//case constants.PipelineRunStatusFault:
			//	ret["startTime"] = runtime.GetStartTime()
			//case constants.PipelineRunStatusBatching:
			//	ret["startTime"] = runtime.GetStartTime()
			//case constants.PipelineRunStatusBatchEnd:
			//	ret["startTime"] = runtime.GetStartTime()
			//	ret["batchEndTime"] = runtime.GetBatchEndTime()
			//case constants.PipelineRunStatusCease:
			//}
		}
		rets = append(rets, ret)
	}

	RespData(c, rets)
}

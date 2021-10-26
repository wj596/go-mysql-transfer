package service

import (
	"fmt"

	"github.com/juju/errors"

	"go-mysql-transfer/dao"
	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/domain/vo"
	"go-mysql-transfer/util/dateutils"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/snowflake"
)

type PipelineInfoService struct {
	dao         *dao.PipelineInfoDao
	sourceDao   *dao.SourceInfoDao
	endpointDao *dao.EndpointInfoDao
	ruleDao     *dao.TransformRuleDao
}

func (s *PipelineInfoService) InitStartStreams() error {
	infos, err := s.dao.SelectList(vo.NewPipelineInfoParams())
	if err != nil {
		return err
	}

	if len(infos) == 0 {
		log.Info("当前没有需要初始化的管道")
	}

	for _, info := range infos {
		if constants.PipelineInfoStatusDisable == info.Status {
			continue //停用
		}

		persist := _streamStateService.GetStreamState(info.Id)
		if constants.PipelineRunStatusCease == persist.RunStatus {
			continue //停止
		}

		runtime := bo.CreatePipelineRunState(info, persist) //创建运行时状况
		var serv *StreamService
		serv, err = createStreamService(info, runtime)
		if err != nil {
			msg := fmt.Sprintf("创建StreamService[%s]失败[%s]", info.Name, err.Error())
			runtime.SetStatusFault(msg)
			log.Error(msg)
			continue
		}

		err = serv.startup()
		if err != nil {
			msg := fmt.Sprintf("启动StreamService[%s]失败[%s]", info.Name, err.Error())
			runtime.SetStatusFault(msg)
			log.Error(msg)
			continue
		}
	}

	return nil
}

func (s *PipelineInfoService) StartStream(id uint64) error {
	pipeline, err := s.Get(id)
	if err != nil {
		return err
	}

	if constants.PipelineInfoStatusDisable == pipeline.Status {
		return errors.Errorf("Pipeline[%s]未启用", pipeline.Name)
	}

	runtime, exist := bo.GetPipelineRunState(id)
	if exist {
		if runtime.IsRunning() {
			return errors.Errorf("StreamService[%s]已经启动，无需重复启动", pipeline.Name)
		}
		if runtime.IsBatching() {
			return errors.Errorf("Pipeline[%s]正在执行全量任务，请等待全量任务结束方可启动", pipeline.Name)
		}
	} else {
		persist := _streamStateService.GetStreamState(id)
		runtime = bo.CreatePipelineRunState(pipeline, persist)
	}

	var serv *StreamService
	serv, err = createStreamService(pipeline, runtime)
	if err != nil {
		runtime.SetStatusFault(err.Error())
		log.Errorf("创建StreamService[%s]失败[%s]", pipeline.Name, err.Error())
		return err
	}

	err = serv.startup()
	if err != nil {
		runtime.SetStatusFault(err.Error())
		log.Errorf("启动StreamService[%s]失败[%s]", pipeline.Name, err.Error())
		return err
	}

	err = _streamStateService.SaveRunningStatus(id, runtime)
	if err != nil {
		runtime.SetStatusFault(err.Error())
		log.Errorf("保存StreamService[%s]运行状态失败[%s]", pipeline.Name, err.Error())
		closeStreamService(serv, err.Error())
		return err
	}

	return nil
}

func (s *PipelineInfoService) StopStream(id uint64) {
	runtime, exist := bo.GetPipelineRunState(id)
	if !exist || !runtime.IsRunning() {
		return
	}

	var serv *StreamService
	serv, exist = getStreamService(id)
	if !exist {
		return
	}

	closeStreamService(serv, "")
}

func (s *PipelineInfoService) StartBatch(id uint64) error {
	pipeline, err := s.Get(id)
	if err != nil {
		return err
	}

	runtime, exist := bo.GetPipelineRunState(id)
	if exist && runtime.IsRunning() {
		return errors.Errorf("请先停止Pipeline[%s]实时监听，才能开始全量同步任务", pipeline.Name)
	}

	if _, batchExist := bo.GetBatchRunState(); batchExist {
		return errors.Errorf("当前有全量同步任务正在执行，请在当前任务运行完毕后再开始新任务")
	}

	var serv *BatchService
	serv, err = createBatchService(id, runtime)
	if err != nil {
		return err
	}

	go func() {
		if err := serv.start(); err != nil {
			fmt.Println(err)
		}
	}()

	return nil
}

func (s *PipelineInfoService) Insert(entity *po.PipelineInfo, rules []*po.TransformRule) error {
	entity.Id, _ = snowflake.NextId()
	entity.CreateTime = dateutils.NowFormatted()
	entity.UpdateTime = dateutils.NowFormatted()
	entity.Status = constants.PipelineInfoStatusEnable //初始状态

	for _, rule := range rules {
		rule.Id, _ = snowflake.NextId()
		rule.PipelineInfoId = entity.Id
	}

	return s.dao.Insert(entity, rules)
}

func (s *PipelineInfoService) UpdateEntity(entity *po.PipelineInfo, rules []*po.TransformRule) error {
	entity.UpdateTime = dateutils.NowFormatted()

	for _, rule := range rules {
		rule.Id, _ = snowflake.NextId()
		rule.PipelineInfoId = entity.Id
	}

	return s.dao.UpdateEntity(entity, rules)
}

// UpdateStatus 设置状态
func (s *PipelineInfoService) UpdateStatus(id uint64, status uint32) error {
	runtime, exist := bo.GetPipelineRunState(id)
	fmt.Println(runtime.ToString())
	if constants.PipelineInfoStatusDisable == status {
		if exist {
			if runtime.IsRunning() || runtime.IsFault() {
				return errors.Errorf("请先停止Pipeline[%s]", runtime.GetPipelineName())
			}
			if runtime.IsBatching() {
				return errors.Errorf("请先等待Pipeline[%s]处理完全量任务", runtime.GetPipelineName())
			}
		}
		bo.RemovePipelineRunState(id)
	}
	return s.dao.UpdateStatus(id, status)
}

func (s *PipelineInfoService) Delete(id uint64) error {
	runtime, exist := bo.GetPipelineRunState(id)
	if exist {
		if runtime.IsRunning() || runtime.IsFault() {
			return errors.Errorf("请先停止Pipeline[%s]", runtime.GetPipelineName())
		}
		if runtime.IsBatching() {
			return errors.Errorf("请先等待Pipeline[%s]处理完全量任务", runtime.GetPipelineName())
		}
		bo.RemovePipelineRunState(id)
	}
	return s.dao.Delete(id)
}

func (s *PipelineInfoService) Get(id uint64) (*po.PipelineInfo, error) {
	return s.dao.Get(id)
}

func (s *PipelineInfoService) GetByParam(params *vo.PipelineInfoParams) (*po.PipelineInfo, error) {
	return s.dao.GetByParam(params)
}

func (s *PipelineInfoService) SelectList(params *vo.PipelineInfoParams) ([]*po.PipelineInfo, error) {
	return s.dao.SelectList(params)
}

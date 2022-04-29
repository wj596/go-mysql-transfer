/*
 * Copyright 2021-2022 the original author(https://github.com/wj596)
 *
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package service

import (
	"fmt"
	"time"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"

	"go-mysql-transfer/dao"
	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/domain/vo"
	"go-mysql-transfer/util/commons"
	"go-mysql-transfer/util/dateutils"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/snowflake"
)

type PipelineInfoService struct {
	dao         *dao.PipelineInfoDao
	sourceDao   *dao.SourceInfoDao
	endpointDao *dao.EndpointInfoDao
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
		if constants.PipelineInfoStatusDisable == info.Status { //停用状态
			continue
		}

		exist, _ := _stateService.existState(info.Id)
		if !exist { //未启动状态
			continue
		}

		state, _ := _stateService.GetState(info.Id)
		if constants.PipelineRunStatusInitial == state.Status { //未启动状态
			continue
		}

		var runtime *bo.PipelineRuntime
		runtime, err = _stateService.GetOrCreateRuntime(info.Id) //创建运行时
		if err != nil {
			log.Error(err.Error())
			continue
		}

		if constants.PipelineRunStatusClose == state.Status { //关闭状态
			runtime.SetCloseStatus()
			continue
		}

		if constants.PipelineRunStatusPanic == state.Status { //错误状态
			runtime.SetPanicStatus("")
			continue
		}

		// ----启动----
		var sourceInfo *po.SourceInfo
		sourceInfo, err = _sourceInfoService.Get(info.SourceId)
		if err != nil {
			continue
		}

		var endpointInfo *po.EndpointInfo
		endpointInfo, err = _endpointInfoService.Get(info.EndpointId)
		if err != nil {
			continue
		}

		var serv *StreamService
		serv, err = createStreamService(sourceInfo, endpointInfo, info, runtime)
		if err != nil {
			cause := fmt.Sprintf("创建StreamService[%s]失败[%s]", info.Name, err.Error())
			_stateService.updateStateByFail(info.Id, runtime, cause)
			_alarmService.failAlarm(info, cause) //告警
			continue
		}

		err = serv.startup()
		if err != nil {
			cause := fmt.Sprintf("启动StreamService[%s]失败[%s]", info.Name, err.Error())
			_stateService.updateStateByFail(info.Id, runtime, cause)
			_alarmService.failAlarm(info, cause) //告警
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

	var runtime *bo.PipelineRuntime
	runtime, err = _stateService.GetOrCreateRuntime(id) //创建运行时
	if err != nil {
		log.Error(err.Error())
		return err
	}

	if runtime.IsRunning() {
		return errors.Errorf("管道已启动，无需重复启动")
	}

	if runtime.IsBatching() {
		return errors.Errorf("正在进行数据全量同步，请等待全量同步结束方可启动", pipeline.Name)
	}

	var sourceInfo *po.SourceInfo
	sourceInfo, err = _sourceInfoService.Get(pipeline.SourceId)
	if err != nil {
		return err
	}

	var endpointInfo *po.EndpointInfo
	endpointInfo, err = _endpointInfoService.Get(pipeline.EndpointId)
	if err != nil {
		return err
	}

	log.Infof("创建[%s]StreamService,SourceInfo: Addr[%s]、User[%s]、Charset[%s]、Flavor[%s]、ServerID[%d]", pipeline.Name, fmt.Sprintf("%s:%d", sourceInfo.Host, sourceInfo.Port), sourceInfo.Username, sourceInfo.Charset, sourceInfo.Flavor, sourceInfo.SlaveID)
	log.Infof("创建[%s]StreamService,EndpointInfo: Type[%s]、Addr[%s]、User[%s]", pipeline.Name, commons.GetEndpointTypeName(endpointInfo.GetType()), endpointInfo.GetAddresses(), endpointInfo.GetUsername())

	var serv *StreamService
	serv, err = createStreamService(sourceInfo, endpointInfo, pipeline, runtime)
	if err != nil {
		cause := fmt.Sprintf("创建StreamService[%s]失败[%s]", pipeline.Name, err.Error())
		_stateService.updateStateByFail(id, runtime, cause)
		_alarmService.failAlarm(pipeline, cause) //告警
		return err
	}

	err = serv.startup()
	if err != nil {
		cause := fmt.Sprintf("启动StreamService[%s]失败[%s]", pipeline.Name, err.Error())
		_stateService.updateStateByFail(id, runtime, cause)
		_alarmService.failAlarm(pipeline, cause) //告警
		return err
	}

	return nil
}

func (s *PipelineInfoService) StopStream(id uint64) {
	runtime, exist := _stateService.getRuntime(id)
	if !exist {
		return
	}

	if runtime.IsInitial() || runtime.IsFail() || runtime.IsClose() || runtime.IsPanic() || runtime.IsBatchEnd() {
		return
	}

	var serv *StreamService
	serv, exist = getStreamService(id)
	if !exist {
		return
	}

	streamServiceClose(serv)
}

func (s *PipelineInfoService) StartBatch(id uint64) error {
	if _, exist := _stateService.getBatchingRuntime(); exist {
		return errors.Errorf("当前有数据全量同步任务正在执行，请在数据全量同步任务运行完毕后再开始新任务")
	}

	pipeline, err := _pipelineInfoService.Get(id)
	if err != nil {
		return err
	}

	var runtime *bo.PipelineRuntime
	runtime, err = _stateService.GetOrCreateRuntime(id) //创建运行时
	if err != nil {
		log.Error(err.Error())
		return err
	}

	if runtime.IsRunning() || runtime.IsFault() {
		return errors.Errorf("请先停止该管道，才能开始全量同步任务", pipeline.Name)
	}

	var sourceInfo *po.SourceInfo
	sourceInfo, err = _sourceInfoService.Get(pipeline.SourceId)
	if err != nil {
		return err
	}

	var endpointInfo *po.EndpointInfo
	endpointInfo, err = _endpointInfoService.Get(pipeline.EndpointId)
	if err != nil {
		return err
	}

	log.Infof("创建BatchService[%s],  SourceInfo: Addr[%s]、User[%s]、Charset[%s]、Flavor[%s]、ServerID[%d]", pipeline.Name, fmt.Sprintf("%s:%d", sourceInfo.Host, sourceInfo.Port), sourceInfo.Username, sourceInfo.Charset, sourceInfo.Flavor, sourceInfo.SlaveID)
	log.Infof("创建BatchService[%s],  EndpointInfo: Type[%s]、Addr[%s]、User[%s]", pipeline.Name, commons.GetEndpointTypeName(endpointInfo.GetType()), endpointInfo.GetAddresses(), endpointInfo.GetUsername())

	var serv *BatchService
	serv, err = createBatchService(sourceInfo, endpointInfo, pipeline, runtime)
	if err != nil {
		return err
	}

	go func(s *BatchService, r *bo.PipelineRuntime) {
		if e := s.startup(); e != nil {
			r.LatestMessage.Store(e.Error())
			return
		}
	}(serv, runtime)

	return nil
}

func (s *PipelineInfoService) SetPosition(pipelineId uint64, pos mysql.Position) error {
	runtime, exist := _stateService.getRuntime(pipelineId)
	if exist && (runtime.IsRunning() || runtime.IsFault() || runtime.IsBatching()) {
		return errors.Errorf("请先停止管道")
	}
	return _positionService.update(pipelineId, pos)
}

func (s *PipelineInfoService) Insert(entity *po.PipelineInfo) error {
	entity.Id, _ = snowflake.NextId()
	entity.CreateTime = dateutils.NowFormatted()
	entity.UpdateTime = dateutils.NowFormatted()
	entity.Status = constants.PipelineInfoStatusEnable //初始状态

	if IsClusterAndLeader() {
		err := s.dao.SyncInsert(entity)
		if err == nil {
			s.sendSyncEvent(entity.Id, 0)
		}
		return err
	}

	return s.dao.Save(entity)
}

func (s *PipelineInfoService) UpdateEntity(entity *po.PipelineInfo) error {
	entity.UpdateTime = dateutils.NowFormatted()
	if IsClusterAndLeader() {
		v, err := s.dao.SyncUpdate(entity)
		if err == nil {
			s.sendSyncEvent(entity.Id, v)
		}
		return err
	}
	return s.dao.Save(entity)
}

// UpdateStatus 设置状态
func (s *PipelineInfoService) UpdateStatus(id uint64, status uint32) error {
	pipeline, err := s.dao.Get(id)
	if err != nil {
		return err
	}

	if constants.PipelineInfoStatusDisable == status {
		if _, exist := _stateService.getRuntime(id); exist {
			_stateService.removeRuntime(id)
		}
	}

	pipeline.Status = status
	if IsClusterAndLeader() {
		var version int32
		version, err = s.dao.SyncUpdate(pipeline)
		if err == nil {
			s.sendSyncEvent(pipeline.Id, version)
		}
		return err
	}

	return s.dao.Save(pipeline)
}

func (s *PipelineInfoService) Delete(id uint64) error {
	if _, exist := _stateService.getRuntime(id); exist {
		_stateService.removeRuntime(id)
	}

	if IsClusterAndLeader() {
		err := s.dao.SyncDelete(id)
		if err == nil {
			s.sendSyncEvent(id, -1)
		}
		return err
	}

	return s.dao.Delete(id)
}

func (s *PipelineInfoService) sendSyncEvent(id uint64, dataVersion int32) {
	_leaderService.sendEvent(&bo.SyncEvent{
		MetadataId:   id,
		MetadataType: constants.MetadataTypePipeline,
		DataVersion:  dataVersion,
		Timestamp:    time.Now().Unix(),
	})
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

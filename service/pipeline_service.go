package service

import (
	"go-mysql-transfer/util/log"
	"sync"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"

	"go-mysql-transfer/dao"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/domain/vo"
	"go-mysql-transfer/util/dateutils"
	"go-mysql-transfer/util/snowflake"
)

type PipelineInfoService struct {
	dao         dao.PipelineInfoDao
	sourceDao   dao.SourceInfoDao
	endpointDao dao.EndpointInfoDao
	ruleDao     dao.TransformRuleDao
	infoService *PipelineInfoService
	dumpers     map[uint64]*dumper
	lock        sync.Mutex
}

func (s *PipelineInfoService) Initialize() error {
	infos, err := s.dao.SelectList("")
	if err != nil {
		return err
	}

	for _, info := range infos {
		if constants.PipelineInfoStatusRunning == info.Status {
			err = s.Startup(info.Id)
			if err != nil {
				log.Errorf(err.Error())
				return err
			}
		}
	}

	return nil
}

func (s *PipelineInfoService) Startup(id uint64) error {
	info, err := s.Get(id)
	if err != nil {
		return err
	}

	if dump, exist := s.dumpers[id]; exist && !dump.isDestroyed() {
		return errors.Errorf("Pipeline[%s]已经启动，无需重复启动", info.Name)
	}

	var dump *dumper
	dump, err = newDumper(id)
	if err != nil {
		return err
	}

	err = dump.start()
	if err != nil {
		return err
	}

	err = s.UpdateStatus(info.Id, constants.PipelineInfoStatusRunning)
	if err != nil {
		dump.destroy()
		dump = nil
		return err
	}
	s.dumpers[id] = dump

	return nil
}

func (s *PipelineInfoService) GetDumperInfo(id uint64) (*vo.PipelineInstanceInfoVO, error) {
	dump, ok := s.dumpers[id]
	if !ok {
		return nil, errors.New("Pipeline实例不存在")
	}

	pos, err := s.GetPosition(id)
	if nil != err {
		return nil, err
	}

	r := new(vo.PipelineInstanceInfoVO)
	r.StartTime = dateutils.DefaultLayout(dump.startTime)
	r.PosName = pos.Name
	r.PosIndex = pos.Pos

	return r, err
}

func (s *PipelineInfoService) Insert(entity *po.PipelineInfo, rules []*po.TransformRule) error {
	entity.Id, _ = snowflake.NextId()
	entity.CreateTime = dateutils.NowFormatted()
	entity.UpdateTime = dateutils.NowFormatted()
	entity.Status = constants.PipelineInfoStatusInitialized

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

func (s *PipelineInfoService) UpdateStatus(id uint64, status uint32) error {
	return s.dao.UpdateStatus(id, status)
}

func (s *PipelineInfoService) UpdatePosition(id uint64, pos mysql.Position) error {
	return s.dao.UpdatePosition(id, pos)
}

func (s *PipelineInfoService) Delete(id uint64) error {
	return s.dao.Delete(id)
}

func (s *PipelineInfoService) Get(id uint64) (*po.PipelineInfo, error) {
	return s.dao.Get(id)
}

func (s *PipelineInfoService) GetByName(name string) (*po.PipelineInfo, error) {
	return s.dao.GetByName(name)
}

func (s *PipelineInfoService) GetPosition(id uint64) (mysql.Position, error) {
	return s.dao.GetPosition(id)
}

func (s *PipelineInfoService) SelectList(name string) ([]*po.PipelineInfo, error) {
	return s.dao.SelectList(name)
}

func (s *PipelineInfoService) GetBySourceAndEndpoint(sourceId, endpointId uint64) (*po.PipelineInfo, error) {
	return s.dao.GetBySourceAndEndpoint(sourceId, endpointId)
}

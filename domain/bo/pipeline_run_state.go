package bo

import (
	"github.com/siddontang/go-mysql/mysql"
	"sync"

	"go.uber.org/atomic"

	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/util/dateutils"
	"go-mysql-transfer/util/stringutils"
)

// PipelineRunState 运行状况
type PipelineRunState struct {
	pipelineId    uint64
	pipelineName  string
	status        *atomic.Uint32
	startTime     *atomic.String
	positionName  *atomic.String
	positionIndex *atomic.Uint32
	insertCounter *atomic.Uint64
	updateCounter *atomic.Uint64
	deleteCounter *atomic.Uint64
	message       *atomic.String

	batchStartTime     *atomic.String
	batchEndTime       *atomic.String
	batchInsertCounter *atomic.Uint64
}

type pipelineRunStateManager struct {
	lock   sync.RWMutex
	states map[uint64]*PipelineRunState
}

var _prm = &pipelineRunStateManager{
	states: make(map[uint64]*PipelineRunState, 0),
}

func CreatePipelineRunState(pipeline *po.PipelineInfo, persist po.StreamState) *PipelineRunState {
	return _prm.create(pipeline.Id, pipeline.Name, persist)
}

func GetPipelineRunState(pipelineId uint64) (*PipelineRunState, bool) {
	return _prm.get(pipelineId)
}

func RemovePipelineRunState(pipelineId uint64) {
	_prm.remove(pipelineId)
}

func GetBatchRunState() (*PipelineRunState, bool) {
	return _prm.getBatchRunning()
}

func (s *pipelineRunStateManager) create(pipelineId uint64, pipelineName string, persist po.StreamState) *PipelineRunState {
	s.lock.Lock()
	defer s.lock.Unlock()

	state := &PipelineRunState{
		pipelineId:    pipelineId,
		pipelineName:  pipelineName,
		status:        atomic.NewUint32(0),
		startTime:     atomic.NewString(""),
		positionName:  atomic.NewString(""),
		positionIndex: atomic.NewUint32(0),
		insertCounter: atomic.NewUint64(persist.InsertCount),
		updateCounter: atomic.NewUint64(persist.UpdateCount),
		deleteCounter: atomic.NewUint64(persist.DeleteCount),
		message:       atomic.NewString(""),
	}
	s.states[pipelineId] = state
	return state
}

func (s *pipelineRunStateManager) get(pipelineId uint64) (*PipelineRunState, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	state, ok := s.states[pipelineId]
	return state, ok
}

func (s *pipelineRunStateManager) getBatchRunning() (*PipelineRunState, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	for _, state := range s.states {
		if state.status.Load() == constants.PipelineRunStatusBatching {
			return state, true
		}
	}

	return nil, false
}

func (s *pipelineRunStateManager) remove(pipelineId uint64) {
	s.lock.Lock()
	defer s.lock.Unlock()

	delete(s.states, pipelineId)
}

func (s *PipelineRunState) InitStartTime() {
	if s.startTime.Load() == "" {
		s.startTime.Store(dateutils.NowFormatted())
	}
}

func (s *PipelineRunState) IsRunning() bool {
	return constants.PipelineRunStatusRunning == s.status.Load()
}

func (s *PipelineRunState) IsFault() bool {
	return s.status.Load() == constants.PipelineRunStatusFault
}

func (s *PipelineRunState) IsCease() bool {
	return s.status.Load() == constants.PipelineRunStatusCease
}

func (s *PipelineRunState) IsBatching() bool {
	return s.status.Load() == constants.PipelineRunStatusBatching
}

func (s *PipelineRunState) GetStatus() uint32 {
	return s.status.Load()
}

func (s *PipelineRunState) SetStatusRunning() {
	s.SetMessage("")
	s.status.Store(constants.PipelineRunStatusRunning)
}

func (s *PipelineRunState) SetStatusFault(cause string) {
	s.SetMessage(cause)
	s.status.Store(constants.PipelineRunStatusFault)
}

func (s *PipelineRunState) SetStatusCease(cause string) {
	s.SetMessage(cause)
	s.status.Store(constants.PipelineRunStatusCease)
}

func (s *PipelineRunState) SetPosition(pos mysql.Position) {
	s.positionName.Store(pos.Name)
	s.positionIndex.Store(pos.Pos)
}

func (s *PipelineRunState) SetStatusBatching() {
	s.batchStartTime.Store(dateutils.NowFormatted())
	s.status.Store(constants.PipelineRunStatusBatching)
}

func (s *PipelineRunState) SetStatusBatchEnd() {
	s.batchEndTime.Store(dateutils.NowFormatted())
	s.status.Store(constants.PipelineRunStatusBatchEnd)
}

func (s *PipelineRunState) GetPipelineName() string {
	return s.pipelineName
}

func (s *PipelineRunState) SetMessage(message string) {
	s.message.Store(message)
}

func (s *PipelineRunState) AddInsertCount(n int) uint64 {
	return s.insertCounter.Add(uint64(n))
}

func (s *PipelineRunState) GetInsertCount() uint64 {
	return s.insertCounter.Load()
}

func (s *PipelineRunState) AddUpdateCount(n int) uint64 {
	return s.updateCounter.Add(uint64(n))
}

func (s *PipelineRunState) GetUpdateCount() uint64 {
	return s.updateCounter.Load()
}

func (s *PipelineRunState) AddDeleteCount(n int) uint64 {
	return s.deleteCounter.Add(uint64(n))
}

func (s *PipelineRunState) GetDeleteCount() uint64 {
	return s.deleteCounter.Load()
}

func (s *PipelineRunState) AddBatchInsertCount(n int) uint64 {
	return s.batchInsertCounter.Add(uint64(n))
}

func (s *PipelineRunState) GetStartTime() string {
	return s.startTime.Load()
}

func (s *PipelineRunState) GetBatchEndTime() string {
	if s.batchEndTime==nil{
		return ""
	}
	return s.batchEndTime.Load()
}

func (s *PipelineRunState) GetPositionName() string {
	if s.positionName==nil{
		return ""
	}
	return s.positionName.Load()
}

func (s *PipelineRunState) GetPositionIndex() uint32 {
	if s.positionIndex==nil{
		return 0
	}
	return s.positionIndex.Load()
}

func (s *PipelineRunState) GetMessage() string {
	return s.message.Load()
}

func (s *PipelineRunState) ToString() string {
	str := "\n StreamRuntimeState["
	str += "\n pipelineId:" + stringutils.ToString(s.pipelineId)
	str += "\n pipelineName:" + s.pipelineName
	str += "\n status:" + stringutils.ToString(s.status.Load())
	str += "\n startTime:" + s.startTime.Load()
	str += "\n positionName:" + s.positionName.Load()
	str += "\n positionIndex:" + stringutils.ToString(s.positionIndex.Load())
	str += "\n insertCounter:" + stringutils.ToString(s.insertCounter.Load())
	str += "\n updateCounter:" + stringutils.ToString(s.updateCounter.Load())
	str += "\n deleteCounter:" + stringutils.ToString(s.deleteCounter.Load())
	str += "\n batchStartTime:" + s.batchStartTime.Load()
	str += "\n batchEndTime:" + s.batchEndTime.Load()
	str += "\n batchInsertCounter:" + stringutils.ToString(s.batchInsertCounter.Load())
	str += "\n message:" + s.message.Load()
	str += "\n ]\n"
	return str
}
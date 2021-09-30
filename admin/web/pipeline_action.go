package web

import (
	"fmt"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/juju/errors"

	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/domain/vo"
	"go-mysql-transfer/service"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/stringutils"
)

type PipelineInfoAction struct {
	service         *service.PipelineInfoService
	sourceService   *service.SourceInfoService
	endpointService *service.EndpointInfoService
}

func initPipelineInfoAction(r *gin.Engine) {
	s := &PipelineInfoAction{
		service:         service.GetPipelineInfoService(),
		sourceService:   service.GetSourceInfoService(),
		endpointService: service.GetEndpointInfoService(),
	}
	r.POST("pipelines", s.Insert)
	r.PUT("pipelines", s.Update)
	r.DELETE("pipelines/:id", s.DeleteBy)
	r.GET("pipelines/:id", s.GetBy)
	r.GET("pipelines", s.Select)
	r.GET("pipelines/:id/get-position", s.GetPosition)

	r.PUT("pipelines/:id/startup", s.Startup)
	r.PUT("pipelines/:id/stop", s.Stop)
	r.PUT("pipelines/:id/position", s.SetPosition)
	r.PUT("pipelines/:id/full-sync", s.FullSync)
	r.GET("pipelines/:id/dumper", s.DumperInfo)
}

func (s *PipelineInfoAction) Insert(c *gin.Context) {
	vo := new(vo.PipelineInfoVO)
	if err := c.BindJSON(vo); err != nil {
		log.Errorf("新增失败: %s", errors.ErrorStack(err))
		Err400(c, err.Error())
		return
	}

	if err := s.check(vo, false); err != nil {
		log.Errorf("新增失败: %s", errors.ErrorStack(err))
		Err400(c, err.Error())
		return
	}

	entity := vo.ToPO()
	rules := make([]*po.TransformRule, len(vo.Rules))
	for i, v := range vo.Rules {
		vv := v.ToPO()
		rules[i] = vv
	}

	if err := s.service.Insert(entity, rules); err != nil {
		log.Errorf("新增失败: %s", errors.ErrorStack(err))
		Err500(c, err.Error())
		return
	}

	RespOK(c)
}

func (s *PipelineInfoAction) Update(c *gin.Context) {
	vo := new(vo.PipelineInfoVO)
	if err := c.BindJSON(vo); err != nil {
		log.Errorf("更新失败: %s", errors.ErrorStack(err))
		Err400(c, err.Error())
		return
	}

	if err := s.check(vo, true); err != nil {
		log.Errorf("更新失败: %s", errors.ErrorStack(err))
		Err400(c, err.Error())
		return
	}

	entity := vo.ToPO()
	rules := make([]*po.TransformRule, len(vo.Rules))
	for i, v := range vo.Rules {
		vv := v.ToPO()
		rules[i] = vv
	}

	if err := s.service.UpdateEntity(entity, rules); err != nil {
		log.Errorf("更新失败: %s", errors.ErrorStack(err))
		Err500(c, err.Error())
		return
	}

	RespOK(c)
}

func (s *PipelineInfoAction) DeleteBy(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("id"))
	if err := s.service.Delete(id); err != nil {
		log.Errorf("删除失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}
	RespOK(c)
}

func (s *PipelineInfoAction) GetBy(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("id"))
	vo, err := s.service.Get(id)
	if nil != err {
		log.Errorf("获取数据失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}
	RespData(c, vo)
}

func (s *PipelineInfoAction) Select(c *gin.Context) {
	name := c.Query("name")
	items, err := s.service.SelectList(name)
	if nil != err {
		log.Errorf("获取数据失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}

	vs := make([]*vo.PipelineInfoVO, 0, len(items))
	for _, item := range items {
		v := new(vo.PipelineInfoVO)
		v.FromPO(item)
		s.padding(v)
		fmt.Println("paddingpaddingpadding::", stringutils.ToJsonIndent(v))
		vs = append(vs, v)
	}

	RespData(c, vs)
}

func (s *PipelineInfoAction) Startup(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("id"))
	err := s.service.Startup(id)
	if nil != err {
		log.Errorf("启动失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}

	RespMsg(c, "启动成功")
}

func (s *PipelineInfoAction) Stop(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("id"))
	err := s.service.UpdateStatus(id, constants.PipelineInfoStatusPause)
	if nil != err {
		log.Errorf("停止失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}
	RespMsg(c, "停止成功")
}

func (s *PipelineInfoAction) GetPosition(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("id"))
	pos, err := s.service.GetPosition(id)
	if nil != err {
		log.Errorf("GetPosition失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}

	RespData(c, pos.Name+"  "+strconv.Itoa(int(pos.Pos)))
}

func (s *PipelineInfoAction) SetPosition(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("id"))
	fmt.Println(id)
	RespMsg(c, "Position设置成功")
}

func (s *PipelineInfoAction) FullSync(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("id"))
	fmt.Println(id)
	RespMsg(c, "Position设置成功")
}

func (s *PipelineInfoAction) DumperInfo(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("id"))
	ret, err := s.service.GetDumperInfo(id)
	if nil != err {
		log.Errorf("GetInstanceInfo失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}
	RespData(c, ret)
}

func (s *PipelineInfoAction) check(pipeline *vo.PipelineInfoVO, update bool) error {
	exist, _ := s.service.GetByName(pipeline.Name)
	if !update {
		if exist != nil {
			return errors.New(fmt.Sprintf("存在名称为[%s]的通道，请更换", pipeline.Name))
		}
		entity, _ := s.service.GetBySourceAndEndpoint(pipeline.SourceId, pipeline.EndpointId)
		if entity != nil {
			vvo := new(vo.PipelineInfoVO)
			vvo.FromPO(entity)
			s.padding(vvo)
			fmt.Println(stringutils.ToJsonIndent(vvo))
			return errors.New(fmt.Sprintf("存在数据源为'%s'，接收端点为'%s'的通道，无需重复创建", vvo.SourceName, vvo.EndpointName))
		}
	} else {
		if exist != nil && exist.Id != pipeline.Id {
			return errors.New(fmt.Sprintf("存在名称为[%s]的通道，请更换", pipeline.Name))
		}
	}

	return nil
}

func (s *PipelineInfoAction) padding(v *vo.PipelineInfoVO) {
	if source, err := s.sourceService.Get(v.SourceId); err == nil {
		v.SourceName = fmt.Sprintf("%s[%s:%d]", source.Name, source.Host, source.Port)
	}
	if endpoint, err := s.endpointService.Get(v.EndpointId); err == nil {
		v.EndpointName = fmt.Sprintf("%s[%s %s]", endpoint.Name, constants.GetEndpointTypeName(endpoint.Type), endpoint.Addresses)
	}
}

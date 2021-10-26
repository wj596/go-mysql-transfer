package web

import (
	"fmt"

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
	r.PUT("pipelines/:id/enable", s.Enable)
	r.PUT("pipelines/:id/disable", s.Disable)
	r.DELETE("pipelines/:id", s.DeleteBy)
	r.GET("pipelines/:id", s.GetBy)
	r.GET("pipelines", s.Select)
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

func (s *PipelineInfoAction) Enable(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("id"))
	err := s.service.UpdateStatus(id, constants.PipelineInfoStatusEnable)
	if err != nil {
		log.Errorf("更新失败: %s", errors.ErrorStack(err))
		Err500(c, err.Error())
		return
	}
	RespOK(c)
}

func (s *PipelineInfoAction) Disable(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("id"))
	err := s.service.UpdateStatus(id, constants.PipelineInfoStatusDisable)
	if err != nil {
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
	params := &vo.PipelineInfoParams{
		Name: c.Query("name"),
	}
	items, err := s.service.SelectList(params)
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
		vs = append(vs, v)
	}

	RespData(c, vs)
}

func (s *PipelineInfoAction) check(pipeline *vo.PipelineInfoVO, update bool) error {
	params := vo.NewPipelineInfoParams()
	params.Name = pipeline.Name
	exist, _ := s.service.GetByParam(params)
	if !update {
		if exist != nil {
			return errors.New(fmt.Sprintf("存在名称为[%s]的通道，请更换", pipeline.Name))
		}
		params.Name = ""
		params.SourceId = pipeline.SourceId
		params.EndpointId = pipeline.EndpointId
		entity, _ := s.service.GetByParam(params)
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

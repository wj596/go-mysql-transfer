package web

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/juju/errors"
	"go-mysql-transfer/model/vo"
	"go-mysql-transfer/service"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/stringutils"
)

type PipelineInfoAction struct {
	service *service.PipelineInfoService
}

func initPipelineInfoAction(r *gin.Engine) {
	s := &PipelineInfoAction{
		service: service.GetPipelineInfoService(),
	}
	r.POST("pipelines", s.Insert)
	r.PUT("pipelines", s.Update)
	r.DELETE("pipelines/:id", s.DeleteBy)
	r.GET("pipelines/:id", s.GetBy)
	r.GET("pipelines", s.SelectPage)
}

func (s *PipelineInfoAction) Insert(c *gin.Context) {
	vo := new(vo.PipelineInfoVO)
	if err := c.BindJSON(vo); err != nil {
		log.Errorf("新增失败: %s", errors.ErrorStack(err))
		Err400(c, err.Error())
		return
	}

	fmt.Println("Insert Pipeline: \n", stringutils.ToJsonIndent(vo))

	if err := s.check(vo, false); err != nil {
		log.Errorf("新增失败: %s", errors.ErrorStack(err))
		Err400(c, err.Error())
		return
	}

	if err := s.service.Insert(vo); err != nil {
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

	fmt.Println("Update Pipeline: \n", stringutils.ToJsonIndent(vo))

	if err := s.check(vo, true); err != nil {
		log.Errorf("更新失败: %s", errors.ErrorStack(err))
		Err400(c, err.Error())
		return
	}

	if err := s.service.Update(vo); err != nil {
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

func (s *PipelineInfoAction) SelectPage(c *gin.Context) {
	term := vo.NewPipelineInfoParams()
	term.SetName(c.Query("name"))
	term.Page().SetCurrentParam(c.Query("page"))
	term.Page().SetLimitParam(c.Query("pageSize"))
	vo, err := s.service.SelectPage(term)
	if nil != err {
		log.Errorf("获取数据失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}
	RespData(c, vo)
}

func (s *PipelineInfoAction) check(vo *vo.PipelineInfoVO, update bool) error {
	exist, _ := s.service.GetByName(vo.Name)
	if exist != nil && !update {
		return errors.New(fmt.Sprintf("存在名称为[%s]的通道，请更换", vo.Name))
	}
	if exist != nil && update && exist.Id != vo.Id {
		return errors.New(fmt.Sprintf("存在名称为[%s]的通道，请更换", vo.Name))
	}
	return nil
}



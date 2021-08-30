package web

import (
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/juju/errors"

	"go-mysql-transfer/model/po"
	"go-mysql-transfer/model/vo"
	"go-mysql-transfer/service"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/stringutils"
	"go-mysql-transfer/util/sysutils"
)

type EndpointInfoAction struct {
	service *service.EndpointInfoService
}

func initEndpointInfoAction(r *gin.Engine) {
	s := &EndpointInfoAction{
		service: service.GetEndpointInfoService(),
	}
	r.POST("endpoints", s.Insert)
	r.POST("endpoints/test_link", s.TestLink)
	r.PUT("endpoints", s.Update)
	r.DELETE("endpoints/:id", s.DeleteBy)
	r.GET("endpoints/:id", s.GetBy)
	r.GET("endpoints", s.Select)
}

func (s *EndpointInfoAction) Insert(c *gin.Context) {
	vo := new(po.EndpointInfo)
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

	if err := s.service.Insert(vo); err != nil {
		log.Errorf("新增失败: %s", errors.ErrorStack(err))
		Err500(c, err.Error())
		return
	}
	RespOK(c)
}

func (s *EndpointInfoAction) Update(c *gin.Context) {
	vo := new(po.EndpointInfo)
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

	if err := s.service.Update(vo); err != nil {
		log.Errorf("更新失败: %s", errors.ErrorStack(err))
		Err500(c, err.Error())
		return
	}
	RespOK(c)
}

func (s *EndpointInfoAction) DeleteBy(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("id"))
	if err := s.service.Delete(id); err != nil {
		log.Errorf("删除失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}
	RespOK(c)
}

func (s *EndpointInfoAction) GetBy(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("id"))
	vo, err := s.service.Get(id)
	if nil != err {
		log.Errorf("获取数据失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}
	RespData(c, vo)
}

func (s *EndpointInfoAction) Select(c *gin.Context) {
	term := vo.NewEndpointInfoParams()
	term.SetName(c.Query("name"))
	term.SetHost(c.Query("host"))
	term.Page().SetCurrentParam(c.Query("page"))
	term.Page().SetLimitParam(c.Query("pageSize"))

	var data interface{}
	var err error
	if term.Page().Necessary() {
		data, err = s.service.SelectPage(term)
	} else {
		data, err = s.service.SelectList(term)
	}

	if nil != err {
		log.Errorf("获取数据失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}
	RespData(c, data)
}

func (s *EndpointInfoAction) TestLink(c *gin.Context) {
	vo := new(po.EndpointInfo)
	if err := c.BindJSON(vo); err != nil {
		log.Errorf("链接测试失败: %s", errors.ErrorStack(err))
		Err400(c, err.Error())
		return
	}

	if err := s.service.TestLink(vo); err != nil {
		log.Errorf("链接测试失败: %s", errors.ErrorStack(err))
		Err500(c, err.Error())
		return
	}
	RespOK(c)
}

func (s *EndpointInfoAction) check(vo *po.EndpointInfo, update bool) error {
	if !sysutils.IsAddresses(vo.GetAddresses()) {
		return errors.Errorf("地址格式不正确,如：127.0.0.1:27017;多个地址用英文逗号分割,如：127.0.0.1:27017,127.0.0.2:27017")
	}
	exist, _ := s.service.GetByName(vo.Name)
	fmt.Println(stringutils.ToJsonIndent(exist))
	if exist != nil && !update {
		fmt.Println(111)
		return errors.New(fmt.Sprintf("存在名称为[%s]的端点，请更换", vo.GetName()))
	}
	if exist != nil && update && exist.Id != vo.Id {
		fmt.Println(222)
		return errors.New(fmt.Sprintf("存在名称为[%s]的端点，请更换", vo.GetName()))
	}
	return nil
}

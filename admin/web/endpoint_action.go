package web

import (
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/juju/errors"

	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/domain/vo"
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
	r.POST("endpoints/test-link", s.TestLink)
	r.PUT("endpoints", s.Update)
	r.DELETE("endpoints/:id", s.DeleteBy)
	r.GET("endpoints/:id", s.GetBy)
	r.GET("endpoints", s.Select)
}

func (s *EndpointInfoAction) Insert(c *gin.Context) {
	vo := new(vo.EndpointInfoVO)
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

	if err := s.service.Insert(vo.ToPO()); err != nil {
		log.Errorf("新增失败: %s", errors.ErrorStack(err))
		Err500(c, err.Error())
		return
	}
	RespOK(c)
}

func (s *EndpointInfoAction) Update(c *gin.Context) {
	vo := new(vo.EndpointInfoVO)
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

	if err := s.service.Update(vo.ToPO()); err != nil {
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
	po, err := s.service.Get(id)
	if nil != err {
		log.Errorf("获取数据失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}

	vo := new(vo.EndpointInfoVO)
	vo.FromPO(po)

	RespData(c, vo)
}

func (s *EndpointInfoAction) Select(c *gin.Context) {
	name := c.Query("name")
	host := c.Query("host")
	list, err := s.service.SelectList(name, host)

	if nil != err {
		log.Errorf("获取数据失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}

	vos := make([]*vo.EndpointInfoVO, 0, len(list))
	for _, l := range list {
		vo := new(vo.EndpointInfoVO)
		vo.FromPO(l)
		vos = append(vos, vo)
	}

	RespData(c, vos)
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
		Err500(c, fmt.Sprintf("链接失败：%s", err.Error()))
		return
	}
	RespOK(c)
}

func (s *EndpointInfoAction) check(vo *vo.EndpointInfoVO, update bool) error {
	if !sysutils.IsAddresses(vo.Addresses) {
		return errors.Errorf("地址格式不正确,如：127.0.0.1:27017;多个地址用英文逗号分割,如：127.0.0.1:27017,127.0.0.2:27017")
	}

	exist, _ := s.service.GetByName(vo.Name)
	if exist != nil && !update {
		return errors.New(fmt.Sprintf("存在名称为[%s]的端点，请更换", vo.Name))
	}
	if exist != nil && update && exist.Id != vo.Id {
		return errors.New(fmt.Sprintf("存在名称为[%s]的端点，请更换", vo.Name))
	}
	return nil
}

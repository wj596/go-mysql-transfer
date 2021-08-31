package web

import (
	"fmt"

	"github.com/asaskevich/govalidator"
	"github.com/gin-gonic/gin"
	"github.com/juju/errors"

	"go-mysql-transfer/model/po"
	"go-mysql-transfer/service"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/stringutils"
)

type SourceInfoAction struct {
	service *service.SourceInfoService
}

func initSourceInfoAction(r *gin.Engine) {
	s := &SourceInfoAction{
		service: service.GetSourceInfoService(),
	}
	r.POST("sources", s.Insert)
	r.POST("sources/test_link", s.TestLink)
	r.PUT("sources", s.Update)
	r.DELETE("sources/:id", s.DeleteBy)
	r.GET("sources/by_id/:id", s.GetBy)
	r.GET("sources", s.Select)
	r.GET("sources/metadata/schemas", s.SelectSchemaList)
	r.GET("sources/metadata/tables", s.SelectTableList)
	r.GET("sources/metadata/table_info", s.GetTableInfo)
}

func (s *SourceInfoAction) Insert(c *gin.Context) {
	vo := new(po.SourceInfo)
	if err := c.BindJSON(vo); err != nil {
		log.Errorf("新增失败: %s", errors.ErrorStack(err))
		Err400(c, err.Error())
		return
	}

	if err := s.check(vo); err != nil {
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

func (s *SourceInfoAction) Update(c *gin.Context) {
	vo := new(po.SourceInfo)
	if err := c.BindJSON(vo); err != nil {
		log.Errorf("更新失败: %s", errors.ErrorStack(err))
		Err400(c, err.Error())
		return
	}

	if err := s.check(vo); err != nil {
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

func (s *SourceInfoAction) DeleteBy(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("id"))
	if err := s.service.Delete(id); err != nil {
		log.Errorf("删除失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}
	RespOK(c)
}

func (s *SourceInfoAction) GetBy(c *gin.Context) {
	id := stringutils.ToUint64Safe(c.Param("id"))
	vo, err := s.service.Get(id)
	if nil != err {
		log.Errorf("获取数据失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}
	RespData(c, vo)
}

func (s *SourceInfoAction) Select(c *gin.Context) {
	name := c.Query("name")
	host := c.Query("host")

	list, err := s.service.SelectList(name, host)
	if nil != err {
		log.Errorf("获取数据失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}
	RespData(c, list)
}

func (s *SourceInfoAction) SelectSchemaList(c *gin.Context) {
	sourceId := stringutils.ToUint64Safe(c.Query("sourceId"))
	fmt.Println(sourceId)
	data,err := s.service.SelectSchemaList(sourceId)
	if nil != err {
		log.Errorf("获取数据失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}
	RespData(c, data)
}

func (s *SourceInfoAction) SelectTableList(c *gin.Context) {
	sourceId := stringutils.ToUint64Safe(c.Query("sourceId"))
	schema := c.Query("schema")
	data,err := s.service.SelectTableList(sourceId, schema)
	if nil != err {
		log.Errorf("获取数据失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}
	RespData(c, data)
}

func (s *SourceInfoAction) GetTableInfo(c *gin.Context) {
	sourceId := stringutils.ToUint64Safe(c.Query("sourceId"))
	schema := c.Query("schema")
	table := c.Query("table")
	data,err := s.service.SelectTableInfo(sourceId, schema, table)
	if nil != err {
		log.Errorf("获取数据失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}
	RespData(c, data)
}

func (s *SourceInfoAction) TestLink(c *gin.Context) {
	vo := new(po.SourceInfo)
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

func (s *SourceInfoAction) check(vo *po.SourceInfo) error {
	if !govalidator.IsIP(vo.GetHost()) {
		return errors.New("主机 IP格式不正确")
	}

	exist, _ := s.service.GetByName(vo.Name)
	if exist != nil && exist.Id != vo.Id {
		return errors.New(fmt.Sprintf("存在名称为[%s]的数据源，请更换", vo.GetName()))
	}

	return nil
}

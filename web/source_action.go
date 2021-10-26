package web

import (
	"fmt"
	"github.com/asaskevich/govalidator"
	"github.com/gin-gonic/gin"
	"github.com/juju/errors"

	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/domain/vo"
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
	r.POST("sources/test-connect", s.TestConnect)
	r.PUT("sources", s.Update)
	r.DELETE("sources/:id", s.DeleteBy)
	r.GET("sources/entity/:id", s.GetBy)
	r.GET("sources", s.Select)
	r.GET("sources/metadata/schemas", s.SelectSchemaList)
	r.GET("sources/metadata/tables", s.SelectTableList)
	r.GET("sources/metadata/table-info", s.GetTableInfo)
}

func (s *SourceInfoAction) Insert(c *gin.Context) {
	vo := new(vo.SourceInfoVO)

	//https://www.cnblogs.com/ahfuzhang/p/14208643.html
	//err = c.ShouldBindBodyWith(&req, binding.ProtoBuf)

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

	if err := s.service.Insert(vo.ToPO()); err != nil {
		log.Errorf("新增失败: %s", errors.ErrorStack(err))
		Err500(c, err.Error())
		return
	}
	RespOK(c)
}

func (s *SourceInfoAction) Update(c *gin.Context) {
	vo := new(vo.SourceInfoVO)
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

	if err := s.service.Update(vo.ToPO()); err != nil {
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
	po, err := s.service.Get(id)
	if nil != err {
		log.Errorf("获取数据失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}

	vo := new(vo.SourceInfoVO)
	vo.FromPO(po)

	RespData(c, vo)
}

func (s *SourceInfoAction) Select(c *gin.Context) {
	params := &vo.SourceInfoParams{
		Name: c.Query("name"),
		Host: c.Query("host"),
	}
	list, err := s.service.SelectList(params)
	if nil != err {
		log.Errorf("获取数据失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}

	vos := make([]*vo.SourceInfoVO, 0, len(list))
	for _, l := range list {
		vo := new(vo.SourceInfoVO)
		vo.FromPO(l)
		vos = append(vos, vo)
	}

	RespData(c, vos)
}

func (s *SourceInfoAction) SelectSchemaList(c *gin.Context) {
	sourceId := stringutils.ToUint64Safe(c.Query("sourceId"))
	fmt.Println(sourceId)
	data, err := s.service.SelectSchemaList(sourceId)
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
	data, err := s.service.SelectTableList(sourceId, schema)
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
	data, err := s.service.SelectTableInfo(sourceId, schema, table)
	if nil != err {
		log.Errorf("获取数据失败: %s", err.Error())
		Err500(c, err.Error())
		return
	}
	RespData(c, data)
}

func (s *SourceInfoAction) TestConnect(c *gin.Context) {
	vo := new(po.SourceInfo)
	if err := c.BindJSON(vo); err != nil {
		log.Errorf("链接测试失败: %s", errors.ErrorStack(err))
		Err400(c, err.Error())
		return
	}

	if err := s.service.TestConnect(vo); err != nil {
		log.Errorf("链接测试失败: %s", errors.ErrorStack(err))
		Err500(c, fmt.Sprintf("链接失败：%s", err.Error()))
		return
	}
	RespOK(c)
}

func (s *SourceInfoAction) check(v *vo.SourceInfoVO) error {
	if !govalidator.IsIP(v.Host) {
		return errors.New("主机 IP格式不正确")
	}

	exist, _ := s.service.GetByName(v.Name)
	if exist != nil && exist.Id != v.Id {
		return errors.New(fmt.Sprintf("存在名称为[%s]的数据源，请更换", v.Name))
	}

	return nil
}

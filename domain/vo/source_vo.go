package vo

import (
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/util/pageutils"
)

type SourceInfoParams struct {
	page *pageutils.PageRequest
	Name string
	Host string
}

type SourceInfoResp struct {
	Total int              `json:"total"` // 总条数
	Items []*po.SourceInfo `json:"items"` // 查询结果
}

type SourceInfoVO struct {
	Id       uint64 `json:"id,string"`
	Name     string `json:"name"`     //名称
	Host     string `json:"host"`     //主机
	Port     uint32 `json:"port"`     //端口
	Username string `json:"username"` //用户名
	Password string `json:"password"` //密码
	Charset  string `json:"charset"`  //字符串编码
	SlaveID  uint32 `json:"slaveID"`  //SlaveID
	Flavor   string `json:"flavor"`   // mysql or mariadb,默认mysql
	Status   uint32 `json:"status"`   //状态 0正常 1停用
}

func NewSourceInfoParams() *SourceInfoParams {
	return &SourceInfoParams{
		page: new(pageutils.PageRequest),
	}
}

func NewSourceInfoResp() *SourceInfoResp {
	return &SourceInfoResp{}
}

func (s *SourceInfoParams) SetName(name string) *SourceInfoParams {
	s.Name = name
	return s
}

func (s *SourceInfoParams) SetHost(host string) *SourceInfoParams {
	s.Host = host
	return s
}

func (s *SourceInfoParams) Page() *pageutils.PageRequest {
	return s.page
}

func (s *SourceInfoResp) SetTotal(total int) *SourceInfoResp {
	s.Total = total
	return s
}

func (s *SourceInfoResp) SetItems(items []*po.SourceInfo) *SourceInfoResp {
	s.Items = items
	return s
}

func (s *SourceInfoVO) ToPO() *po.SourceInfo {
	return &po.SourceInfo{
		Id:       s.Id,
		Name:     s.Name,
		Host:     s.Host,
		Port:     s.Port,
		Username: s.Username,
		Password: s.Password,
		Charset:  s.Charset,
		SlaveID:  s.SlaveID,
		Flavor:   s.Flavor,
		Status:   s.Status,
	}
}

func (s *SourceInfoVO) FromPO(p *po.SourceInfo) {
	s.Id = p.Id
	s.Name = p.Name
	s.Host = p.Host
	s.Port = p.Port
	s.Username = p.Username
	s.Password = p.Password
	s.Charset = p.Charset
	s.SlaveID = p.SlaveID
	s.Flavor = p.Flavor
	s.Status = p.Status
}

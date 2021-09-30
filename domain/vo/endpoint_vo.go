package vo

import (
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/util/pageutils"
)

type EndpointInfoParams struct {
	page *pageutils.PageRequest
	Name string
	Host string
}

type EndpointInfoResp struct {
	Total int                `json:"total"` // 总条数
	Items []*po.EndpointInfo `json:"items"` // 查询结果
}

type EndpointInfoVO struct {
	Id        uint64 `json:"id,string"`
	Name      string `json:"name"`
	Type      uint32 `json:"type"`      //端点类型
	Addresses string `json:"addresses"` //地址
	Username  string `json:"username"`
	Password  string `json:"password"`
	// Redis
	DeployType uint32 `json:"deployType"` //部署模式 0：单机、1：集群
	GroupType  uint32 `json:"groupType"`  //集群类型 0：sentinel、1：cluster
	MasterName string `json:"masterName"` //Master节点名称,如果group_type为sentinel则此项不能为空，为cluster此项无效
	Database   uint32 `json:"database"`   //redis数据库 0-16,默认0。如果group_type为cluster此项无效
	//Elasticsearch
	Version uint32 `json:"version"` //Elasticsearch版本，支持6和7、默认为7
	//Rocketmq
	GroupName    string `json:"groupName"`    //rocketmq group name,默认为空
	InstanceName string `json:"instanceName"` //rocketmq instance name,默认为空
	//http接口
	AuthMode     uint32 `json:"authMode"`     //认证模式 0:无效认证 1：Http Basic 2：JWT
	JwtSecretKey string `json:"jwtSecretKey"` //jwt签名秘钥
	JwtExpire    uint32 `json:"jwtExpire"`    //jwt签名有效期（单位秒）
}

func NewEndpointInfoParams() *EndpointInfoParams {
	return &EndpointInfoParams{
		page: new(pageutils.PageRequest),
	}
}

func NewEndpointInfoResp() *EndpointInfoResp {
	return &EndpointInfoResp{}
}

func (s *EndpointInfoParams) SetName(name string) *EndpointInfoParams {
	s.Name = name
	return s
}

func (s *EndpointInfoParams) SetHost(host string) *EndpointInfoParams {
	s.Host = host
	return s
}

func (s *EndpointInfoParams) Page() *pageutils.PageRequest {
	return s.page
}

func (s *EndpointInfoResp) SetTotal(total int) *EndpointInfoResp {
	s.Total = total
	return s
}

func (s *EndpointInfoResp) SetItems(items []*po.EndpointInfo) *EndpointInfoResp {
	s.Items = items
	return s
}

func (s *EndpointInfoVO) ToPO() *po.EndpointInfo {
	return &po.EndpointInfo{
		Id:        s.Id,
		Name:      s.Name,
		Type:      s.Type,
		Addresses: s.Addresses,
		Username:  s.Username,
		Password:  s.Password,
		// Redis
		DeployType: s.DeployType,
		GroupType:  s.GroupType,
		MasterName: s.MasterName,
		Database:   s.Database,
		//Elasticsearch
		Version: s.Version,
		//Rocketmq
		GroupName:    s.GroupName,
		InstanceName: s.InstanceName,
		//http接口
		AuthMode:     s.AuthMode,
		JwtSecretKey: s.JwtSecretKey,
		JwtExpire:    s.JwtExpire,
	}
}

func (s *EndpointInfoVO) FromPO(p *po.EndpointInfo) {
	s.Id = p.Id
	s.Name = p.Name
	s.Type = p.Type
	s.Addresses = p.Addresses
	s.Username = p.Username
	s.Password = p.Password
	// Redis
	s.DeployType = p.DeployType
	s.GroupType = p.GroupType
	s.MasterName = p.MasterName
	s.Database = p.Database
	//Elasticsearch
	s.Version = p.Version
	//Rocketmq
	s.GroupName = p.GroupName
	s.InstanceName = p.InstanceName
	//http接口
	s.AuthMode = p.AuthMode
	s.JwtSecretKey = p.JwtSecretKey
	s.JwtExpire = p.JwtExpire
}

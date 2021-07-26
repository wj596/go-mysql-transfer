package model

//Type
// 1: mongodb Addr Username Password
// 2: elasticsearch  Addr  User  Password Version
// 3: redis  Addr   GroupType  MasterName Pass Database
// 4: rocketmq NameServers GroupName InstanceName AccessKey SecretKey
// 5: kafka Addr SASLUser SASLPassword
// 6: rabbitmq Addr
// 7: script

const (
	TargetTypeMongodb       = 1
	TargetTypeElasticsearch = 2
	TargetTypeRedis         = 3
	TargetTypeRocketmq      = 4
	TargetTypeKafka         = 5
	TargetTypeRabbitmq      = 6
	TargetTypeScript        = 7
)

// TargetInfo 目标
type TargetInfo struct {
	Id       uint64
	Type     uint8
	Name     string
	Addr     string //地址
	Username string //用户名
	Password string //密码

	GroupType  string `yaml:"group_type"`  //集群类型 sentinel或者cluster
	MasterName string `yaml:"master_name"` //Master节点名称
	Database   int    `yaml:"database"`    //数据库

	Version int //版本

	NameServers  string //命名服务地址，多个用逗号分隔
	GroupName    string //group name,默认为空
	InstanceName string //instance name,默认为空
	AccessKey    string //访问控制 accessKey,默认为空
	SecretKey    string //访问控制 secretKey,默认为空

	SASLUser     string `yaml:"sasl_user"`     //SASL_PLAINTEXT认证模式 用户名
	SASLPassword string `yaml:"sasl_password"` //SASL_PLAINTEXT认证模式 密码

}

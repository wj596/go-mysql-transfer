package config

// ClusterConfig 集群配置
type ClusterConfig struct {
	Name             string `yaml:"name"`    //绑定IP
	BindIp           string `yaml:"bind_ip"` //绑定IP
	ZkAddrs          string `yaml:"zk_addrs"`
	ZkAuthentication string `yaml:"zk_authentication"`
	EtcdAddrs        string `yaml:"etcd_addrs"`
	EtcdUser         string `yaml:"etcd_user"`
	EtcdPassword     string `yaml:"etcd_password"`
}

func (c *ClusterConfig) GetName() string {
	return c.Name
}

func (c *ClusterConfig) GetBindIp() string {
	return c.BindIp
}

func (c *ClusterConfig) GetZkAddrs() string {
	return c.ZkAddrs
}

func (c *ClusterConfig) GetZkAuthentication() string {
	return c.ZkAuthentication
}

func (c *ClusterConfig) GetEtcdAddrs() string {
	return c.EtcdAddrs
}

func (c *ClusterConfig) GetEtcdUser() string {
	return c.EtcdUser
}

func (c *ClusterConfig) GetEtcdPassword() string {
	return c.EtcdPassword
}

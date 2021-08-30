package config

type ConsumerConfig struct {
	Name     string `yaml:"name"`     //用户名
	Password string `yaml:"password"` //密码
	Role     string `yaml:"role"`     //角色 admin|viewer
}

func (c *ConsumerConfig) GetName() string {
	return c.Name
}

func (c *ConsumerConfig) GetPassword() string {
	return c.Password
}

func (c *ConsumerConfig) GetRole() string {
	return c.Role
}

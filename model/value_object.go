package model

type LoginVO struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type RoleVO struct {
	RoleName string `json:"roleName"`
	Value    string `json:"value"`
}

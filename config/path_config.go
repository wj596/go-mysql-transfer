package config

import (
	"go-mysql-transfer/util/stringutils"
	"strings"
)

const (
	_position = "/position" // position
)

func GetRootNode() string {
	return "/" + _clusterName
}

func GetPositionNode(pipeline string) string {
	if pipeline == "" {
		return join(GetRootNode(), _position)
	}
	return join(GetRootNode(), pipeline, _position)
}

func join(args ...interface{}) string {
	path := ""
	for _, arg := range args {
		if arg != "" {
			p := stringutils.ToString(arg)
			if strings.HasPrefix(p, "/") {
				path = path + p
			} else {
				path = path + "/" + p
			}
		}
	}
	return path
}

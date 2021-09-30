package path

import (
	"strings"

	"go-mysql-transfer/config"
	"go-mysql-transfer/util/stringutils"
)

const (
	_position = "/position" // position
)

func GetRootPath() string {
	return "/" + config.GetIns().GetClusterConfig().GetName()
}

func GetPositionPath(pipeline string) string {
	if pipeline == "" {
		return join(GetRootPath(), _position)
	}
	return join(GetRootPath(), pipeline, _position)
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

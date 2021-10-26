package dao

import (
	"strings"

	"go-mysql-transfer/config"
	"go-mysql-transfer/util/stringutils"
)

const (
	_position = "/position" // position
	_commitIndex = "/commit_index" // commitIndex
)

func getRootNode() string {
	return "/" + config.GetIns().GetClusterConfig().GetName()
}

func getCommitIndex() string {
	return join(getRootNode(), _commitIndex)
}

func getPositionNode(pipeline string) string {
	if pipeline == "" {
		return join(getRootNode(), _position)
	}
	return join(getRootNode(), pipeline, _position)
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

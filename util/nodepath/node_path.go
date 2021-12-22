package nodepath

import (
	"strconv"
	"strings"

	"go-mysql-transfer/util/stringutils"
)

const (
	_root             = "/go-mysql-transfer"
	_state            = "/state"
	_position         = "/position"
	_election         = "/election"
	_machine          = "/machine"
	_metadata         = "/metadata"
	_metadataSource   = "/metadata/source"
	_metadataEndpoint = "/metadata/endpoint"
	_metadataRule     = "/metadata/rule"
	_metadataPipeline = "/metadata/pipeline"
)

func GetRootNode() string {
	return _root
}

func GetMachineParentNode() string {
	return join(_root, _machine)
}

func GetMachineNode(node string) string {
	return join(_root, _machine, node)
}

func GetElectionNode() string {
	return join(_root, _election)
}

func GetPositionParentNode() string {
	return join(_root, _position)
}

func GetStateParentNode() string {
	return join(_root, _state)
}

func GetPositionNode(pipelineId uint64) string {
	return join(_root, _position, strconv.FormatUint(pipelineId, 10))
}

func GetStateNode(pipelineId uint64) string {
	return join(_root, _state, strconv.FormatUint(pipelineId, 10))
}

func GetMetadataRootNode() string {
	return join(_root, _metadata)
}

func GetMetadataParentNode(metadataType string) string {
	return join(_root, _metadata, metadataType)
}

func GetMetadataNode(metadataType string, id uint64) string {
	return join(_root, _metadata, metadataType, strconv.FormatUint(id, 10))
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

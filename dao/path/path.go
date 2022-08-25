package path

import (
	"go-mysql-transfer/util/stringutils"
	"strconv"
	"strings"
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

func GetRoot() string {
	return _root
}

func GetMachineRoot() string {
	return join(_root, _machine)
}

func CreateMachinePath(node string) string {
	return join(_root, _machine, node)
}

func GetElectionRoot() string {
	return join(_root, _election)
}

func GetPositionRoot() string {
	return join(_root, _position)
}

func CreatePositionPath(pipelineId uint64) string {
	return join(_root, _position, strconv.FormatUint(pipelineId, 10))
}

func GetStateRoot() string {
	return join(_root, _state)
}

func CreateStatePath(pipelineId uint64) string {
	return join(_root, _state, strconv.FormatUint(pipelineId, 10))
}

func GetMetadataRoot() string {
	return join(_root, _metadata)
}

func GetSourceMetadataRoot() string {
	return join(_root, _metadataSource)
}

func CreateSourceMetadataPath(id uint64) string {
	return join(_root, _metadataSource, strconv.FormatUint(id, 10))
}

func GetEndpointMetadataRoot() string {
	return join(_root, _metadataEndpoint)
}

func CreateEndpointMetadataPath(id uint64) string {
	return join(_root, _metadataEndpoint, strconv.FormatUint(id, 10))
}

func GetPipelineMetadataRoot() string {
	return join(_root, _metadataPipeline)
}

func CreatePipelineMetadataPath(id uint64) string {
	return join(_root, _metadataPipeline, strconv.FormatUint(id, 10))
}

func GetRuleMetadataRoot() string {
	return join(_root, _metadataRule)
}

func CreateRuleMetadataParentPath(pipelineId uint64) string {
	return join(_root, _metadataRule, strconv.FormatUint(pipelineId, 10))
}

func CreateRuleMetadataItemPath(pipelineId uint64, ruleId string) string {
	return join(_root, _metadataRule, strconv.FormatUint(pipelineId, 10), ruleId)
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

/*
 * Copyright 2021-2022 the original author(https://github.com/wj596)
 *
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package zookeeper

import (
	"github.com/go-zookeeper/zk"
	"google.golang.org/protobuf/proto"

	"go-mysql-transfer/dao/path"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/util/gziputils"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/stringutils"
	"go-mysql-transfer/util/zkutils"
)

type PipelineDaoImpl struct {
}

func (s *PipelineDaoImpl) Insert(entity *po.PipelineInfo) error {
	if err := zkutils.CreateNodeIfNecessary(path.CreateRuleMetadataParentPath(entity.Id), _connection); err != nil {
		return err
	}

	for _, rule := range entity.Rules {
		marshaled, err := proto.Marshal(rule)
		if err != nil {
			return err
		}
		var gzip []byte
		gzip, err = gziputils.Zip(marshaled)
		if err != nil {
			return err
		}
		node := path.CreateRuleMetadataItemPath(entity.Id, stringutils.UUID())
		_, err = _connection.Create(node, gzip, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return err
		}
	}

	copied := po.DeepCopyPipelineInfo(entity)
	marshaled, err := proto.Marshal(copied)
	if err != nil {
		return err
	}
	var gzip []byte
	gzip, err = gziputils.Zip(marshaled)
	if err != nil {
		return err
	}

	node := path.CreatePipelineMetadataPath(entity.Id)
	_, err = _connection.Create(node, gzip, 0, zk.WorldACL(zk.PermAll))
	return err
}

func (s *PipelineDaoImpl) Delete(id uint64) error {
	parentNode := path.CreateRuleMetadataParentPath(id)
	ruleIds, _, err := _connection.Children(parentNode)
	if err != nil {
		return err
	}
	for _, ruleId := range ruleIds {
		node := path.CreateRuleMetadataItemPath(id, ruleId)
		err = _connection.Delete(node, -1)
		if err != nil {
			return err
		}
	}
	err = _connection.Delete(parentNode, -1)
	if err != nil {
		return err
	}
	return _connection.Delete(path.CreatePipelineMetadataPath(id), -1)
}

func (s *PipelineDaoImpl) Update(entity *po.PipelineInfo, version int32) error {
	// -------------- 删除RULES ----------------
	ruleParentNode := path.CreateRuleMetadataParentPath(entity.Id)
	ruleIds, _, err := _connection.Children(ruleParentNode)
	if err != nil {
		return err
	}
	for _, ruleId := range ruleIds {
		node := path.CreateRuleMetadataItemPath(entity.Id, ruleId)
		err = _connection.Delete(node, -1)
		if err != nil {
			return err
		}
	}
	err = _connection.Delete(ruleParentNode, -1)
	if err != nil {
		return err
	}

	// -------------- 保存RULES ----------------
	if err := zkutils.CreateNodeIfNecessary(path.CreateRuleMetadataParentPath(entity.Id), _connection); err != nil {
		return err
	}

	for _, rule := range entity.Rules {
		marshaled, err := proto.Marshal(rule)
		if err != nil {
			return err
		}
		var gzip []byte
		gzip, err = gziputils.Zip(marshaled)
		if err != nil {
			return err
		}
		node := path.CreateRuleMetadataItemPath(entity.Id, stringutils.UUID())
		_, err = _connection.Create(node, gzip, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return err
		}
	}

	// -------------- 保存PIPE ----------------
	var marshaled []byte
	copied := po.DeepCopyPipelineInfo(entity)
	marshaled, err = proto.Marshal(copied)
	if err != nil {
		return err
	}
	var gzip []byte
	gzip, err = gziputils.Zip(marshaled)
	if err != nil {
		return err
	}

	node := path.CreatePipelineMetadataPath(entity.Id)
	_, err = _connection.Set(node, gzip, version)
	return err
}

func (s *PipelineDaoImpl) GetDataVersion(id uint64) (int32, error) {
	node := path.CreatePipelineMetadataPath(id)
	exist, state, err := _connection.Exists(node)
	if err != nil {
		return 0, err
	}
	if exist {
		return state.Version, nil
	}
	return 0, nil
}

func (s *PipelineDaoImpl) Get(id uint64) (*po.PipelineInfo, error) {
	ruleParentNode := path.CreateRuleMetadataParentPath(id)
	ruleIds, _, err := _connection.Children(ruleParentNode)
	if err != nil {
		return nil, err
	}

	rules := make([]*po.Rule, len(ruleIds))
	for _, ruleId := range ruleIds {
		node := path.CreateRuleMetadataItemPath(id, ruleId)
		var temp []byte
		temp, _, err = _connection.Get(node)
		if err != nil {
			return nil, err
		}
		var data []byte
		data, err = gziputils.UnZip(temp)
		if err != nil {
			return nil, err
		}
		var rule po.Rule
		err = proto.Unmarshal(data, &rule)
		if err != nil {
			return nil, err
		}
		rules = append(rules, &rule)
	}

	node := path.CreatePipelineMetadataPath(id)
	temp, stat, err := _connection.Get(node)
	if err != nil {
		return nil, err
	}

	var data []byte
	data, err = gziputils.UnZip(temp)
	if err != nil {
		return nil, err
	}

	var entity po.PipelineInfo
	err = proto.Unmarshal(data, &entity)
	if err != nil {
		return nil, err
	}
	entity.DataVersion = stat.Version

	return &entity, nil
}

func (s *PipelineDaoImpl) SelectAllDataVersion() ([]*po.MetadataVersion, error) {
	root := path.GetPipelineMetadataRoot()
	keys, _, err := _connection.Children(root)
	if err != nil {
		log.Errorf("查询所有[PipelineInfo]节点失败[%s]", err.Error())
		return nil, err
	}

	ls := make([]*po.MetadataVersion, 0)
	for _, key := range keys {
		node := path.GetPipelineMetadataRoot() + "/" + key
		_, stat, err := _connection.Exists(node)
		if err != nil {
			return nil, err
		}

		ls = append(ls, &po.MetadataVersion{
			Id:      stringutils.ToUint64Safe(key),
			Version: stat.Version,
		})
	}

	return ls, nil
}

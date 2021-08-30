package zkutils

import (
	"github.com/go-zookeeper/zk"

	"go-mysql-transfer/util/log"
)

func CreateNodeIfNecessary(node string, conn *zk.Conn) error {
	exist, _, err := conn.Exists(node)
	if err != nil {
		return err
	}

	if exist {
		log.Infof("存在节点[%s],未创建", node)
		return nil
	}

	log.Infof("不存在节点[%s],创建", node)
	_, err = conn.Create(node, nil, 0, zk.WorldACL(zk.PermAll))
	return err
}

func CreateNodeWithDataIfNecessary(node string, data []byte, conn *zk.Conn) error {
	exist, _, err := conn.Exists(node)
	if err != nil {
		return err
	}

	if exist {
		log.Infof("存在节点[%s],未创建", node)
		return nil
	}

	log.Infof("不存在节点[%s],创建", node)
	_, err = conn.Create(node, data, 0, zk.WorldACL(zk.PermAll))
	return err
}

func CreateNodeWithFlagIfNecessary(node string, flags int32, conn *zk.Conn) error {
	exist, _, err := conn.Exists(node)
	if err != nil {
		return err
	}

	if exist {
		log.Infof("存在节点[%s],未创建", node)
		return nil
	}

	log.Infof("不存在节点[%s],创建", node)
	_, err = conn.Create(node, nil, flags, zk.WorldACL(zk.PermAll))
	return err
}

func DeleteNode(node string, conn *zk.Conn) error {
	log.Infof("删除节点[%s]", node)
	return conn.Delete(node, -1)
}

func SetNode(node string, data []byte, conn *zk.Conn) error {
	log.Infof("修改节点[%s]", node)
	_, stat, err := conn.Get(node)
	if err != nil {
		return err
	}

	_, err = conn.Set(node, data, stat.Version)
	return err
}

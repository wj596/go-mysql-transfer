package dao

import (
	"fmt"
	"github.com/go-zookeeper/zk"
	"testing"
)

func TestVersionInfo(t *testing.T) {
	before(t)

	//err := zkutils.CreateNodeIfNecessary("/commit_index" ,_zkConn)
	//if nil!=err {
	//	t.Fatal(err)
	//}

	_, stat, err := _zkConn.Get("/commit_index")
	if err != nil {
		t.Fatal(err)
	}

	stat,err = _zkConn.Set("/commit_index",marshalId(1),stat.Version)
	if nil!=err {
		t.Fatal(err)
	}
	fmt.Println(stat)
}

func TestZkSequence(t *testing.T) {
	before(t)

	//_, err := _zkConn.Create("/translog", nil, 0, zk.WorldACL(zk.PermAll))
	//if nil!=err {
	//	t.Fatal(err)
	//}

	node, err := _zkConn.Create("/translog/", nil, zk.FlagSequence, zk.WorldACL(zk.PermAll))
	if nil!=err {
		t.Fatal(err)
	}
	fmt.Println(node)

	node, err = _zkConn.Create("/translog/", nil, zk.FlagSequence, zk.WorldACL(zk.PermAll))
	if nil!=err {
		t.Fatal(err)
	}
	fmt.Println(node)

	//
	//err := zkutils.CreateNodeIfNecessary("/commit_index" ,_zkConn)
	//if nil!=err {
	//	t.Fatal(err)
	//}
	//
	//_, stat, err := _zkConn.Get("/commit_index")
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//stat,err = _zkConn.Set("/translog",marshalId(1),stat.Version)
	//if nil!=err {
	//	t.Fatal(err)
	//}
	//fmt.Println(stat)
	//
	//_zkConn.Create()
}
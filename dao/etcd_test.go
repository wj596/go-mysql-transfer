package dao

import (
	"context"
	"go.etcd.io/etcd/clientv3"
	"testing"
	"time"
)

func TestEtcdSequence(t *testing.T) {
	before(t)

	ctx, cancel := context.WithTimeout(context.Background(), 1 * time.Second)
	defer cancel()
	_, err := _etcdOps.Txn(ctx).If(
		clientv3.Compare(clientv3.ModRevision("queue/a"), "=", 0),
	).Then(
		clientv3.OpPut("", "1"),
	).Commit()

	if err != nil {
		t.Fatal(err)
	}
}

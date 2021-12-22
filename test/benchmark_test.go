package test

import (
	"encoding/json"
	"go-mysql-transfer/domain/po"
	"google.golang.org/protobuf/proto"
	"testing"
)

func BenchmarkDirectAccess(b *testing.B) {
	e := &po.PipelineState{
		Status:      1,
		InsertCount: 1250122,
		UpdateCount: 2652,
		DeleteCount: 55855596699,
		Node:        "192.168.44.113",
		UpdateTime:  2365892585559,
	}

	for jj := 0; jj < b.N; jj++ {
		proto.Marshal(e)
	}
}

func BenchmarkMethod(b *testing.B) {
	e := &po.PipelineState{
		Status:      1,
		InsertCount: 1250122,
		UpdateCount: 2652,
		DeleteCount: 55855596699,
		Node:        "192.168.44.113",
		UpdateTime:  2365892585559,
	}

	for jj := 0; jj < b.N; jj++ {
		json.Marshal(e)
	}
}

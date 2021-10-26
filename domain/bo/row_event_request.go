package bo

import "sync"

type RowEventRequest struct {
	Context       *RuleContext
	Action    string
	Timestamp uint32
	PreData   []interface{} //变更之前的数据
	Data      []interface{} //当前的数据
}

var rowEventRequestPool = sync.Pool{
	New: func() interface{} {
		return new(RowEventRequest)
	},
}

func BorrowRowEventRequest() *RowEventRequest {
	return rowEventRequestPool.Get().(*RowEventRequest)
}

func ReleaseRowEventRequest(r *RowEventRequest) {
	r.Context = nil
	r.Action = ""
	r.Timestamp = 0
	r.PreData = nil
	r.Data = nil
	rowEventRequestPool.Put(r)
}

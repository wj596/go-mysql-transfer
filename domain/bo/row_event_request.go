package bo

import "sync"

type RowEventRequest struct {
	RuleKey   string
	Action    string
	Timestamp uint32
	Covered   []interface{}
	Row       []interface{}
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
	r.Action = ""
	r.RuleKey = ""
	r.Timestamp = 0
	r.Covered = nil
	r.Row = nil
	rowEventRequestPool.Put(r)
}

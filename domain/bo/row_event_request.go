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

package bo

import "sync"

type RowEventRequest struct {
	Context   *RuleContext
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

package luaengine

import (
	"strconv"
	"sync"

	"github.com/sony/sonyflake"
)

var (
	_sf   *sonyflake.Sonyflake
	_lock sync.Mutex
)

func nextId() string {
	if _sf != nil {
		id, _ := _sf.NextID()
		return strconv.FormatUint(id, 10)
	}

	_lock.Lock()
	defer _lock.Unlock()

	if _sf == nil {
		var st sonyflake.Settings
		st.MachineID = func() (u uint16, e error) {
			return 0, nil
		}
		_sf = sonyflake.NewSonyflake(st)
	}

	id, _ := _sf.NextID()
	return strconv.FormatUint(id, 10)
}

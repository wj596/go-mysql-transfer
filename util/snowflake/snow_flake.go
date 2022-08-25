package snowflake

import (
	"strconv"
	"sync"

	"github.com/sony/sonyflake"

	"go-mysql-transfer/util/log"
)

var (
	_sf   *sonyflake.Sonyflake
	_lock sync.Mutex
)

func Initialize(machineId uint16) {
	if _sf != nil {
		return
	}

	var st sonyflake.Settings
	st.MachineID = func() (u uint16, e error) {
		return machineId, nil
	}
	_sf = sonyflake.NewSonyflake(st)
}

func NextId() (uint64, error) {
	id, err := _sf.NextID()
	if err != nil {
		log.Errorf("snowflake NextId ：%s", err.Error())
	}
	return id, nil
}

func NextStrId() string {
	id, err := _sf.NextID()
	if err != nil {
		log.Errorf("snowflake NextId ：%s", err.Error())
		return ""
	}
	return strconv.FormatUint(id, 10)
}

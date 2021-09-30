package incrementer

import (
	"go.uber.org/atomic"
	"strconv"
)

var _inc atomic.Uint64

func Next() uint64 {
	return _inc.Inc()
}

func NextStr() string {
	return strconv.FormatUint(_inc.Inc(), 10)
}

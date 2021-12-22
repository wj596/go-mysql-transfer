package collections

import "go-mysql-transfer/util/stringutils"

func Contain(array []string, v interface{}) bool {
	vvv := stringutils.ToString(v)
	for _, vv := range array {
		if vv == vvv {
			return true
		}
	}
	return false
}

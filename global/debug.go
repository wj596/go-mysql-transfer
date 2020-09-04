package global

import (
	"fmt"
	"go-mysql-transfer/util/stringutil"
)

func Debug(msg string, data interface{}) {
	fmt.Println(msg, " :: \n", stringutil.ToJsonString(data))
}

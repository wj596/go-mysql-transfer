package commons

import (
	"fmt"
	"testing"
)

func TestGetDataSourceName(t *testing.T) {
	dsn := GetDataSourceName("root","root","192.168.44.113","%s",3306,"")
	println(dsn)
	println(fmt.Sprintf(dsn,"baseapi"))
}

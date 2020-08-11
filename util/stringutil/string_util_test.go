package stringutil

import (
	"fmt"
	"testing"

	stringu "github.com/pingcap/tidb/util/stringutil"
)

func TestIsChineseChar(t *testing.T) {
	println(IsChineseChar("a"))
	println(IsChineseChar(","))
	println(IsChineseChar("a我b"))
	println(IsChineseChar("，"))
}

type RowRequest struct {
	RuleKey string
	Action  int
	Row     []interface{}
}

func TestStringCopy(t *testing.T) {
	copies := stringu.Copy("ssssss")
	fmt.Println(copies)
}

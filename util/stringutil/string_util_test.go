package stringutil

import (
	"fmt"
	"strings"
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

func TestIndexof(t *testing.T) {
	str := "ss_sss_s"
	index := strings.Index(str, "_")
	fmt.Println(index)
	fmt.Println(str[index+1 : len(str)])
}


func TestToUint32(t *testing.T) {
	str := "964063387"
	fmt.Println(ToUint32(str))

	str2 := "a964063387"
	fmt.Println(ToUint32(str2))
	fmt.Println(ToUint32Safe(str2))
}
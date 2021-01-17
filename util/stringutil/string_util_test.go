package stringutil

import (
	"testing"
)

func TestIsChineseChar(t *testing.T) {
	println(IsChineseChar("a"))
	println(IsChineseChar(","))
	println(IsChineseChar("a我b"))
	println(IsChineseChar("，"))
}

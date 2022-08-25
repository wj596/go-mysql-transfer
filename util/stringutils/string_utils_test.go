package stringutils

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsChineseChar(t *testing.T) {
	a := assert.New(t)
	a.False(IsChineseChar("a"))
	a.False(IsChineseChar(","))
	a.True(IsChineseChar("a我b"))
	a.True(IsChineseChar("，"))
}

func TestToString(t *testing.T) {
	var f []byte
	f = nil
	s := string(f)
	a := assert.New(t)
	a.Equal(s, "")
}

func TestIsNumber(t *testing.T) {
	println(IsNumber("abc1235"))
	println(IsNumber("1235a"))
	println(IsNumber("12_35"))
	println(IsNumber("1235"))
	println(IsNumber("123.5"))
	vv, _ := strconv.ParseFloat("1", 64)
	println(vv)
}
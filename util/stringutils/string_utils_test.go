package stringutils

import (
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
	a.Equal(s,"")
}

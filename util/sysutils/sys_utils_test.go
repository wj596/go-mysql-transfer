package sysutils

import (
	"fmt"
	"testing"
)

func TestIsAddresses(t *testing.T) {
	b := IsAddresses(":")
	fmt.Println(b)
}

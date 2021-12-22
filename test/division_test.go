package test

import (
	"fmt"
	"testing"
)

func TestDivisions(t *testing.T) {
	a := 1000
	var b float64
	b = 3
	var ff float64
	ff = float64(a) / b
	fmt.Println(ff)
}

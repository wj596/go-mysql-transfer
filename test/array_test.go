package test

import (
	"fmt"
	"testing"
)

func TestIndex(t *testing.T) {
	//rules := make([]string, 2)
	//rules[0] = "a"
	//rules[1] = "b"
	//
	//fmt.Println(rules[3])

	var totalRow int64 = 100
	var batch int64
	var size int64 = 80
	if totalRow%size == 0 {
		batch = totalRow / size
	} else {
		batch = (totalRow / size) + 1
	}
	fmt.Println(batch)
}

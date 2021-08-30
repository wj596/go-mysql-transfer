package pageutils

import (
	"fmt"
	"testing"
)

func TestPageRequest(t *testing.T) {
	list := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23}
	fmt.Println(list[0:10])
	fmt.Println(list[10:20])
	fmt.Println(list[20:])
}

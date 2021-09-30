package incrementer

import (
	"fmt"
	"testing"
)

func TestNextStr(t *testing.T) {
	for i := 0; i <= 10; i++ {
		fmt.Println(NextStr())
	}
}

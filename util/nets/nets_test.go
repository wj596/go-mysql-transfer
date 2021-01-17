package nets

import (
	"fmt"
	"testing"
)

func TestIsUsableAddr(t *testing.T) {
	fmt.Println(IsUsableAddr(":8080"))
	fmt.Println(IsUsableAddr(":8070"))
	fmt.Println(IsUsableAddr(":8060"))
	fmt.Println(IsUsableAddr(":8050"))
}

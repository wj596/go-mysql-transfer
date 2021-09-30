package byteutil

import "testing"
import "fmt"

func TestStringToBytes(t *testing.T) {
	s := "abcdefghi"
	b := StringToBytes(s)
	s2 := BytesToString(b)

	fmt.Println(s, b, s2)
}

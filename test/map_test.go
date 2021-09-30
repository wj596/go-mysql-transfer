package test

import (
	"fmt"
	"testing"
)

func TestMakeMap(t *testing.T) {
	rules := make(map[string]string, 2)
	rules["a"] = "a"
	rules["b"] = "b"
	rules["c"] = "c"
	fmt.Println(rules)

	fmt.Println("s" + "\\." + "t")

}

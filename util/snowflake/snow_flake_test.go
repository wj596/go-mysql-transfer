package snowflake

import (
	"fmt"
	"testing"
)

func TestNextId(t *testing.T) {
	Initialize(1)
	id, err := NextId()
	if err != nil {
		t.Fatal(err.Error())
	}
	fmt.Println(id)
}

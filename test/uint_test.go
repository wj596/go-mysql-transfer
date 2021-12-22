package test

import (
	"fmt"
	"go-mysql-transfer/util/snowflake"
	"go-mysql-transfer/util/stringutils"
	"testing"
)

func TestDivision(t *testing.T) {
	fmt.Println(len(stringutils.UUID()))
	fmt.Println(snowflake.NextId())
}

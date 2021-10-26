package dao

import (
	"github.com/siddontang/go-mysql/mysql"
	"testing"
)

func TestSavePosition(t *testing.T) {
	before(t)

	entity := mysql.Position{
		Name: "ddddd",
		Pos:  123456,
	}
	err := GetStateDao().SavePosition(1, entity)
	if err != nil {
		t.Fatal(err)
	}
}

func TestGetPosition(t *testing.T) {
	before(t)

	pos := GetStateDao().GetPosition(1)
	println(pos.Name)
	println(pos.Pos)
}

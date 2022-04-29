package jsonutils

import (
	"testing"
)

type Person struct {
	Action    string
	Timestamp uint32
	PreData   []interface{} //变更之前的数据
	Data      []interface{} //当前的数据
}

func BenchmarkMapJsoniter(b *testing.B) {
	mymap := make(map[string]string)
	mymap["Name1"] = "xxxxxxxxxxxxx"
	mymap["Name2"] = "xxxxxxxxxxxxxxxxxxxx"
	mymap["Name3"] = "xxxxxxxxxxxxxxxxxx"
	mymap["Name"] = "buickxxxxxxxxxxxxxxxxxxxxxxxccee"
	for i := 0; i < b.N; i++ { //use b.N for looping
		ToJsonByJsoniter(mymap)
	}
	// 2200308	       558.4 ns/op
}

func BenchmarkMapStd(b *testing.B) {
	mymap := make(map[string]string)
	mymap["Name1"] = "xxxxxxxxxxxxx"
	mymap["Name2"] = "xxxxxxxxxxxxxxxxxxxx"
	mymap["Name3"] = "xxxxxxxxxxxxxxxxxx"
	mymap["Name"] = "buickxxxxxxxxxxxxxxxxxxxxxxxccee"
	for i := 0; i < b.N; i++ { //use b.N for looping
		ToJson(mymap)
	}
	// 859419	      1582 ns/op
}

func BenchmarkStructJsoniter(b *testing.B) {
	vo := new(Person)
	vo.Action = "Insert"
	vo.Timestamp = uint32(123556699)
	vo.Data = []interface{}{"buickxxxxxxxxxxxxxxxxxxxxxxxccee"}
	for i := 0; i < b.N; i++ { //use b.N for looping
		ToJsonByJsoniter(vo)
	}
	// 2415978	       494.7 ns/op
}

func BenchmarkStrucStd(b *testing.B) {
	vo := new(Person)
	vo.Action = "Insert"
	vo.Timestamp = uint32(123556699)
	vo.Data = []interface{}{"buickxxxxxxxxxxxxxxxxxxxxxxxccee"}
	for i := 0; i < b.N; i++ { //use b.N for looping
		ToJson(vo)
	}
	// 2523744	       476.1 ns/op
}




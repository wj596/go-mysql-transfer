package jsonutils

import (
	"testing"
	"unsafe"
)

func BenchmarkUnsafeToString(b *testing.B) {
	mymap := make(map[string]string)
	mymap["Name1"] = "xxxxxxxxxxxxx"
	mymap["Name2"] = "xxxxxxxxxxxxxxxxxxxx"
	mymap["Name3"] = "xxxxxxxxxxxxxxxxxx"
	mymap["Name"] = "buickxxxxxxxxxxxxxxxxxxxxxxxccee"
	for i := 0; i < b.N; i++ { //use b.N for looping
		bytes,_ := ToJson(mymap)
		str := *(*string)(unsafe.Pointer(&bytes))
		if str == "" {
			b.Fatal()
		}
	}
}

func BenchmarkToString(b *testing.B) {
	mymap := make(map[string]string)
	mymap["Name1"] = "xxxxxxxxxxxxx"
	mymap["Name2"] = "xxxxxxxxxxxxxxxxxxxx"
	mymap["Name3"] = "xxxxxxxxxxxxxxxxxx"
	mymap["Name"] = "buickxxxxxxxxxxxxxxxxxxxxxxxccee"
	for i := 0; i < b.N; i++ { //use b.N for looping
		bytes,_ := ToJson(mymap)
		str := string(bytes)
		if str == "" {
			b.Fatal()
		}
	}
}

package test

import (
	"encoding/json"
	"github.com/json-iterator/go"
	"testing"
)

//var jsoniters = jsoniter.ConfigCompatibleWithStandardLibrary

func BenchmarkMapJsoniter(b *testing.B) {
	mymap := make(map[string]string, 10000)
	mymap["Name1"] = "xxxxxxxxxxxxx"
	mymap["Name2"] = "xxxxxxxxxxxxxxxxxxxx"
	mymap["Name3"] = "xxxxxxxxxxxxxxxxxx"
	mymap["Name"] = "buickxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
	for i := 0; i < b.N; i++ { //use b.N for looping
		jsoniter.Marshal(mymap)
	}
}

func BenchmarkMapStd(b *testing.B) {
	mymap := make(map[string]string, 10000)
	mymap["Name1"] = "xxxxxxxxxxxxx"
	mymap["Name2"] = "xxxxxxxxxxxxxxxxxxxx"
	mymap["Name3"] = "xxxxxxxxxxxxxxxxxx"
	mymap["Name"] = "buickxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
	for i := 0; i < b.N; i++ { //use b.N for looping
		json.Marshal(mymap)
	}
}

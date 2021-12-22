package gziputils

import (
	"fmt"
	"io/ioutil"
	"testing"
)

func TestGzip(t *testing.T) {
	data, _ := ioutil.ReadFile("D:\\test.txt")
	fmt.Println(len(data))
	zip, _ := Zip(data)
	fmt.Println(len(zip))

	data2, err := UnZip(zip)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(len(data2) == len(data))
	fmt.Println(string(data2) == string(data))
}

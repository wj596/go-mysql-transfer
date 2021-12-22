package test

import (
	"fmt"
	"net/url"
	"testing"
	"time"
)

func TestIndex(t *testing.T) {
	//rules := make([]string, 2)
	//rules[0] = "a"
	//rules[1] = "b"
	//
	//fmt.Println(rules[3])

	//var totalRow int64 = 100
	//var batch int64
	//var size int64 = 80
	//if totalRow%size == 0 {
	//	batch = totalRow / size
	//} else {
	//	batch = (totalRow / size) + 1
	//}
	//fmt.Println(batch)
	secretKey := "sssssss"
	value := url.Values{}
	if secretKey != "" {
		t := time.Now().UnixNano() / 1e6
		value.Set("timestamp", fmt.Sprintf("%d", t))
		value.Set("sign", "ssss")
	}
	fmt.Println(value.Encode())

}

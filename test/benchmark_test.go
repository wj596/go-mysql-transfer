package test

import (
	"fmt"
	"go-mysql-transfer/test/ss"
	"testing"
)

func BenchmarkDirectAccess(b *testing.B) {
	ss.Init()
	for jj := 0; jj < b.N; jj++ {
		p,ok := ss.Persons["50"]
		if !ok{
			fmt.Println(p)
			fmt.Println(ok)
		}
	}
}

func BenchmarkMethod(b *testing.B) {
	ss.Init()
	for jj := 0; jj < b.N; jj++ {
		p,ok := ss.Get("50")
		if !ok{
			fmt.Println(p)
			fmt.Println(ok)
		}
	}
}
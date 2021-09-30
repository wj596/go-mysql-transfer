package test

import (
	"fmt"
	"reflect"
	"testing"
)

func TestCase1(t *testing.T) {

	// Slice 会共享内存
	//foo := make([]int, 5)
	//foo[3] = 42
	//foo[4] = 100
	//bar  := foo[1:4]
	//bar[1] = 99
	//fmt.Println(foo)
	//fmt.Println(bar)

	//append()这个函数在 cap 不够用的时候，就会重新分配内存以扩大容量，
	//如果够用，就不会重新分配内存了！
	a := make([]int, 32)
	b := a[1:16]
	a = append(a, 1)
	a[2] = 42
	fmt.Println(a)
	fmt.Println(b)
}

type data struct {
}

// 深度比较
func TestDeepEqual(t *testing.T) {
	v1 := data{}
	v2 := data{}
	fmt.Println("v1 == v2:", reflect.DeepEqual(v1, v2))
	//prints: v1 == v2: true

	m1 := map[string]string{"one": "a", "two": "b"}
	m2 := map[string]string{"two": "b", "one": "a"}
	fmt.Println("m1 == m2:", reflect.DeepEqual(m1, m2))
	//prints: m1 == m2: true

	s1 := []int{1, 2, 3}
	s2 := []int{1, 2, 3}
	fmt.Println("s1 == s2:", reflect.DeepEqual(s1, s2))
	//prints: s1 == s2: true
}

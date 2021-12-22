package test

import (
	"fmt"
	"sync"
	"testing"
)

var _map sync.Map

func TestSyncMap(t *testing.T) {
	_map.Store("a", 1)
	_map.Store("b", 2)
	_map.Store("c", 3)
	_map.Range(func(key, value interface{}) bool {
		_map.Delete(key)
		return true
	})

	_map.Range(func(key, value interface{}) bool {
		fmt.Println(key, value)
		return true
	})
}

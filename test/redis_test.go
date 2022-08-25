package test

import (
	"github.com/go-redis/redis"
	"testing"
)

const (
	address = "127.0.0.1:6379"
	password = ""
	database = 0
)

func TestSet(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password,
		DB:       int(database),
	})

	_,err := client.Ping().Result()
	if nil != err {
		println(err.Error())
	}

	result, err := client.Set("test_1", 1, -1).Result()
	if nil != err {
		println(err.Error())
	}

	println("result: ",  result)
}

func TestIncr(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password,
		DB:       int(database),
	})

	_,err := client.Ping().Result()
	if nil != err {
		println(err.Error())
	}

	result, err := client.Incr("test_1").Result()
	if nil != err {
		println(err.Error())
	}

	println("result: ",  result)
}

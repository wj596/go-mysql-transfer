package test

import (
	"github.com/go-redis/redis"
	"go-mysql-transfer/util/stringutils"
	"sync"
	"testing"
)

var wg sync.WaitGroup

func TestSingle(t *testing.T) {

	client := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
		DB:   0,
	})
	//pipe1 := client.Pipeline()
	//pipe2 := client.Pipeline()
	//pipe3 := client.Pipeline()

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(_index int) {
			var p redis.Pipeliner
			p = client.Pipeline()
			//if _index==0{
			//	p = pipe1
			//}
			//if _index==1{
			//	p = pipe2
			//}
			//if _index==2{
			//	p = pipe3
			//}
			for j := 0; j < 100000; j++ {
				p.Set(stringutils.ToString(_index)+"_test_"+stringutils.ToString(j), "aaaaaaaaaaaaaaaaaaaaaaaaaassssssssssssssssss", 0)
			}
			p.Exec()
			wg.Done()
		}(i)
	}
	wg.Wait()

}

package global

import (
	"go-mysql-transfer/util/logutil"
	"sync"
	"time"
)

var max int64
var buckets = make(map[int64]int64)
var lock sync.Mutex

func Mark(n int) {
	curr := time.Now().Unix()

	nnn := int64(n)
	nn, ok := buckets[curr]
	if ok {
		buckets[curr] = nnn + nn
	} else {
		buckets[curr] = nnn
	}
}

func InitRateCounter() {
	ticker := time.NewTicker(5 * time.Second)
	go func() {
		for {
			<-ticker.C
			if len(buckets) == 0 {
				continue
			}

			var sum int64
			for k, v := range buckets {
				sum = sum + v
				logutil.Infof("时间桶：%d ，处理条数：%d", k, v)
			}
			rate := sum / int64(len(buckets))
			if rate > max {
				max = rate
			}
			logutil.Infof("近60s平均速率(TPS)：%d", rate)
			logutil.Infof("历史最高速率(TPS)：%d", max)

			curr := time.Now().Unix()
			for k, _ := range buckets {
				if curr-k > 60 {
					delete(buckets, k)
				}
			}
		}
	}()
}

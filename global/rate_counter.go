/*
 * Copyright 2020-2021 the original author(https://github.com/wj596)
 *
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */
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

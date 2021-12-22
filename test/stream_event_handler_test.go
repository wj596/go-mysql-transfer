package test

import (
	"fmt"
	"go.uber.org/atomic"
	"testing"
	"time"
)

var sss *MockStreamEventHandler

type MockStreamEventHandler struct {
	counter    *atomic.Int32
	queue      chan interface{}
	stopSignal chan struct{}
}

func newMockStreamEventHandler() *MockStreamEventHandler {
	return &MockStreamEventHandler{
		counter:    atomic.NewInt32(0),
		queue:      make(chan interface{}, 10),
		stopSignal: make(chan struct{}, 1),
	}
}

func (s *MockStreamEventHandler) start() {
	go func() {
		flushInterval := time.Duration(300)
		ticker := time.NewTicker(time.Millisecond * flushInterval)
		defer ticker.Stop()
		for {
			needFlush := false
			select {
			case v := <-s.queue:
				fmt.Println(v)
			case <-ticker.C:
				needFlush = true
			case <-s.stopSignal:
				fmt.Println("停止事件监听")
				return
			}

			//刷新数据
			if needFlush {
				s.counter.Add(1)
				fmt.Println(s.counter.Load())
				if s.counter.Load() > 10 {
					sss.stop()
					sss = nil
				}
			}
		}
	}()
}

func (s *MockStreamEventHandler) stop() {
	s.stopSignal <- struct{}{}
}

func TestMockStreamEventHandler(t *testing.T) {
	handler := newMockStreamEventHandler()
	sss = handler
	handler.start()
	time.Sleep(1 * time.Hour)
}

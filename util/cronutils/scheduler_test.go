package cronutils

import (
	"fmt"
	"testing"
	"time"
)

type myTask struct {
}

func (t *myTask) Run() {
	fmt.Println("myTask")
}

func TestCronSchedulerStart(t *testing.T) {
	task := &myTask{}
	s, err := NewJobScheduler("*/5 * * * * ?", task)
	if err != nil {
		fmt.Println(err.Error())
	}
	s.Start()

	time.Sleep(10 * time.Second)
}

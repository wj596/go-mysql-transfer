package cronutils

import (
	"time"

	"github.com/robfig/cron"
)

// ValidateCronSpec 验证Cron表达式
func ValidateCronSpec(spec string) error {
	_, err := cron.Parse(spec)
	return err
}

// GetTimeStep 获取两次执行时间的间隔
func GetTimeStep(spec string) int64 {
	now := time.Now()
	actual, _ := cron.Parse(spec)
	next1 := actual.Next(now)
	next2 := actual.Next(next1)
	return next2.Unix() - next1.Unix()
}

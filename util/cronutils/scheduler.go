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

package cronutils

import (
	"time"

	"github.com/robfig/cron"
	"go.uber.org/atomic"
)

// TaskFunc 任务函数
type TaskFunc func()

// Scheduler Cron调度器
type Scheduler struct {
	started    atomic.Bool // 0停止  1运行
	startTime  int64
	spec       string
	cronLab    *cron.Cron
	taskFunc   TaskFunc // 任务
	taskObject cron.Job // 任务
}

// NewFuncScheduler 创建Func调度器
func NewFuncScheduler(spec string, task TaskFunc) (*Scheduler, error) {
	scheduler := &Scheduler{
		taskFunc: task,
		spec:     spec,
	}
	cronLab := cron.New()
	err := cronLab.AddFunc(spec, task)
	if err != nil {
		return nil, err
	}
	scheduler.cronLab = cronLab
	return scheduler, nil
}

// NewJobScheduler 创建Job调度器
func NewJobScheduler(spec string, job cron.Job) (*Scheduler, error) {
	scheduler := &Scheduler{
		taskObject: job,
		spec:       spec,
	}
	cronLab := cron.New()
	err := cronLab.AddJob(spec, job)
	if err != nil {
		return nil, err
	}
	scheduler.cronLab = cronLab
	return scheduler, nil
}

// Start 启动Cron调度器
func (s *Scheduler) Start() {
	if s.started.Load() {
		return
	}
	s.started.Store(true)
	s.startTime = time.Now().Unix()
	s.cronLab.Start()
}

// Stop 止Cron调度器
func (s *Scheduler) Stop() {
	if s.started.Load() {
		s.cronLab.Stop()
		s.started.Store(false)
	}
}

// GetNextTime 获取下次执行时间
func (s *Scheduler) GetNextTime() int64 {
	if s.started.Load() {
		entries := s.cronLab.Entries()
		if nil != entries && nil != entries[0] {
			return entries[0].Next.Unix()
		}
	}
	now := time.Now().In(s.cronLab.Location())
	actual, _ := cron.Parse(s.spec)
	return actual.Next(now).Unix()
}

// GetTaskFunc 任务函数
func (s *Scheduler) GetTaskFunc() TaskFunc {
	return s.taskFunc
}

// GetJob 获取Job
func (s *Scheduler) GetJob() cron.Job {
	return s.taskObject
}

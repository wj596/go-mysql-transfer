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
package collections

import "sync"

type List struct {
	lock sync.RWMutex //互斥锁

	elements []interface{}
	size     int
}

func NewList(values ...interface{}) *List {
	l := &List{}
	if len(values) > 0 {
		l.Add(values...)
	}
	return l
}

func (s *List) within(index int) bool {
	return index >= 0 && index < s.size
}

func (s *List) Add(values ...interface{}) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if len(values) > 0 {
		s.elements = append(s.elements, values)
		s.size = s.size + len(values)
	}
}

func (s *List) Get(index int) interface{} {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if !s.within(index) {
		return nil
	}

	return s.elements[index]
}

func (s *List) Remove(index int) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.within(index) {
		return
	}

	s.elements = append(s.elements[:index], s.elements[index+1:]...)

	s.size--
}

func (s *List) Size() int {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.size
}

/*
 * Copyright 2021-2022 the original author(https://github.com/wj596)
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

package bo

import (
	"sync"
)

type PipelineAllocation struct {
	maps map[string][]uint64
	lock sync.RWMutex
}

func NewPipelineAllocation() *PipelineAllocation {
	return &PipelineAllocation{
		maps: make(map[string][]uint64),
	}
}

func (s *PipelineAllocation) AddNode(node string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	_, ok := s.maps[node]
	if !ok {
		s.maps[node] = make([]uint64, 0)
	}
}

func (s *PipelineAllocation) AddPipeline(node string, pipeline uint64) {
	s.lock.Lock()
	defer s.lock.Unlock()

	exist := false
	for _, ps := range s.maps {
		for _, p := range ps {
			if pipeline == p {
				exist = true
			}
		}
	}

	if !exist {
		pipelines, ok := s.maps[node]
		if !ok {
			pipelines = make([]uint64, 0)
		}
		pipelines = append(pipelines, pipeline)
		s.maps[node] = pipelines
	}
}

func (s *PipelineAllocation) RemovePipeline(node string, pipeline uint64) {
	s.lock.Lock()
	defer s.lock.Unlock()

	pipelines, ok := s.maps[node]
	if ok {
		index := -1
		for i, p := range pipelines {
			if pipeline == p {
				index = i
			}
		}
		if index >= 0 {
			pipelines = append(pipelines[:index], pipelines[index+1:]...)
			s.maps[node] = pipelines
		}
	}
}

func (s *PipelineAllocation) GetNode(pipe uint64) (string, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	for node, pipelines := range s.maps {
		for _, pipeline := range pipelines {
			if pipe == pipeline {
				return node, true
			}
		}
	}
	return "", false
}

func (s *PipelineAllocation) GetPipelines(node string) []uint64 {
	s.lock.RLock()
	defer s.lock.RUnlock()

	pipelines, ok := s.maps[node]
	if ok {
		return pipelines
	}
	return make([]uint64, 0)
}

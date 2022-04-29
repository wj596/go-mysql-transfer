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

package luaengine

import (
	"sync"

	"github.com/layeh/gopher-json"
	"github.com/yuin/gopher-lua"

	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/util/log"
)

var _pool = &luaStatePool{
	states: make([]*lua.LState, 0, 3),
}

type luaStatePool struct {
	lock   sync.Mutex
	states []*lua.LState
}

func (p *luaStatePool) Borrow() *lua.LState {
	p.lock.Lock()
	defer p.lock.Unlock()

	n := len(p.states)
	log.Infof("Borrow LState，Pool Size[%d]", n)

	if n == 0 {
		L := lua.NewState()
		return L
	}
	x := p.states[n-1]
	p.states = p.states[0 : n-1]
	return x
}

func (p *luaStatePool) Release(L *lua.LState) {
	p.lock.Lock()
	defer p.lock.Unlock()

	log.Infof("Release LState，Pool Size[%d]", len(p.states))
	p.states = append(p.states, L)
}

func (p *luaStatePool) Shutdown() {
	for _, L := range p.states {
		L.Close()
	}
	p.states = p.states[0:0]
}

func New(endpointType uint32) *lua.LState {
	L := lua.NewState()

	json.Preload(L)     //加载json模块
	preloadLogModule(L) //加载log模块
	preloadDatabaseClientModule(L) //加载database模块

	switch endpointType {
	case constants.EndpointTypeRedis:
		preloadRedisModule(L) //加载redis模块
	case constants.EndpointTypeMongoDB:
		preloadMongodbModule(L)
	case constants.EndpointTypeRabbitMQ:
		preloadMQModule(L)
	case constants.EndpointTypeRocketMQ:
		preloadMQModule(L)
	case constants.EndpointTypeKafka:
		preloadMQModule(L)
	case constants.EndpointTypeElasticsearch:
		preloadESModule(L)
	case constants.EndpointTypeHttp:
		preloadHttpClientModule(L)
	}

	//L.PreloadModule("scriptOps", scriptModule)
	//L.PreloadModule("dbOps", dbModule)
	//L.PreloadModule("httpOps", httpModule)
	return L
}

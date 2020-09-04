package luaengine

import (
	luaJson "github.com/layeh/gopher-json"
	lua "github.com/yuin/gopher-lua"

	"sync"
)

var _pool = &luaStatePool{
	saved: make([]*lua.LState, 0, 3),
}

type luaStatePool struct {
	lock  sync.Mutex
	saved []*lua.LState
}

func (p *luaStatePool) Get() *lua.LState {
	p.lock.Lock()
	defer p.lock.Unlock()

	n := len(p.saved)
	if n == 0 {
		return p.New()
	}
	x := p.saved[n-1]
	p.saved = p.saved[0 : n-1]
	return x
}

func (p *luaStatePool) New() *lua.LState {
	L := lua.NewState()

	luaJson.Preload(L)

	L.PreloadModule("redisOps", redisModule)
	L.PreloadModule("mqOps", mqModule)
	L.PreloadModule("mongodbOps", mongoModule)
	L.PreloadModule("esOps", esModule)

	return L
}

func (p *luaStatePool) Put(L *lua.LState) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.saved = append(p.saved, L)
}

func (p *luaStatePool) Shutdown() {
	for _, L := range p.saved {
		L.Close()
	}
}

package luaengine

import (
	"fmt"
	"go-mysql-transfer/endpoint/luaengine/redis"
	"sync"

	"github.com/layeh/gopher-json"
	"github.com/yuin/gopher-lua"

	"go-mysql-transfer/endpoint/luaengine/db"
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
	fmt.Println(fmt.Sprintf("LuaStatePool Size: %d", n))
	if n == 0 {
		return p.new()
	}
	x := p.states[n-1]
	p.states = p.states[0 : n-1]
	return x
}

func (p *luaStatePool) Release(L *lua.LState) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.states = append(p.states, L)
}

func (p *luaStatePool) Shutdown() {
	for _, L := range p.states {
		L.Close()
	}
	p.states = p.states[0:0]
}

func (p *luaStatePool) new() *lua.LState {
	L := lua.NewState()

	json.Preload(L)  //加载json模块
	db.Preload(L)    //加载db模块
	redis.Preload(L) //加载redis模块
	//L.PreloadModule("scriptOps", scriptModule)
	//L.PreloadModule("dbOps", dbModule)
	//L.PreloadModule("httpOps", httpModule)
	//L.PreloadModule("redisOps", redisModule)
	//L.PreloadModule("mqOps", mqModule)
	//L.PreloadModule("mongodbOps", mongoModule)
	//L.PreloadModule("esOps", esModule)
	return L
}

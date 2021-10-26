package datasource

import (
	"sync"

	"github.com/siddontang/go-mysql/canal"

	"go-mysql-transfer/domain/po"
)

type ConnectionPool struct {
	index    int64
	lock     sync.Mutex
	cons     []*canal.Canal
	consSize int64
}

func NewConnectionPool(size int, ds *po.SourceInfo) (*ConnectionPool, error) {
	var err error
	cons := make([]*canal.Canal, 0, 3)
	for i := 0; i < size; i++ {
		var con *canal.Canal
		con, err = CreateConnection(ds)
		if err != nil {
			break
		}
		cons = append(cons, con)
	}
	if err != nil {
		for _, con := range cons {
			con.Close()
		}
	}

	return &ConnectionPool{
		cons:     cons,
		consSize: int64(len(cons)),
	}, nil
}

func (s *ConnectionPool) Get() *canal.Canal {
	s.lock.Lock()
	defer s.lock.Unlock()

	i := s.index % s.consSize
	s.index++
	return s.cons[i]
}

func (s *ConnectionPool) Shutdown() {
	for _, con := range s.cons {
		con.Close()
	}
}

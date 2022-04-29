package pool

import (
	"bytes"
	"sync"
)

var (
	_bufferPool = &sync.Pool{New: func() interface{} { return &bytes.Buffer{} }}
)

func GetBuffer() *bytes.Buffer {
	return _bufferPool.Get().(*bytes.Buffer)
}

func ReleaseBuffer(buf *bytes.Buffer) {
	if buf != nil {
		buf.Reset()
		_bufferPool.Put(buf)
	}
}

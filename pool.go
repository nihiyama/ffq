package ffq

import (
	"bytes"
	"sync"
)

var queueBufferSize = 64 * 1024

var (
	indexBufPool = sync.Pool{
		New: func() any {
			var b [12]byte
			return &b
		},
	}
	queueBufPool = sync.Pool{
		New: func() any {
			// default 64kb Pool
			return bytes.NewBuffer(make([]byte, 0, queueBufferSize))
		},
	}
)

func SetQueueBufferSize(size int) {
	queueBufferSize = size
	queueBufPool.New = func() any {
		return bytes.NewBuffer(make([]byte, 0, queueBufferSize))
	}
}

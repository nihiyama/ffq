// Package ffq provides a file-based FIFO queue implementation that supports generic types.
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

// SetQueueBufferSize changes the default buffer size used by queue operations.
// It also updates the sync.Pool `queueBufPool` to create buffers with the new size.
//
// Parameters:
//   - size: The new buffer size in bytes.
//
// Example:
//
//	ffq.SetQueueBufferSize(128 * 1024) // Set buffer size to 128KB
func SetQueueBufferSize(size int) {
	queueBufferSize = size
	queueBufPool.New = func() any {
		return bytes.NewBuffer(make([]byte, 0, queueBufferSize))
	}
}

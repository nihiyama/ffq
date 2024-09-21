package ffq

import (
	"bytes"
	"runtime"
	"sync"
	"testing"
)

func TestSetQueueBufferSize(t *testing.T) {
	tests := []struct {
		name   string
		input  int
		expect int
	}{
		{
			name:   "set buffer size",
			input:  128 * 1024,
			expect: 128 * 1024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// recreate queueBufPool for test
			queueBufPool = sync.Pool{
				New: func() any {
					return bytes.NewBuffer(make([]byte, 0, queueBufferSize))
				},
			}
			SetQueueBufferSize(tt.input)
			runtime.GC()
			buf := queueBufPool.Get().(*bytes.Buffer)
			actual := buf.Cap()
			if tt.expect != actual {
				t.Errorf("Failed test: %s, expect: %d, actual %d", tt.name, tt.expect, actual)
			}
		})
	}
}

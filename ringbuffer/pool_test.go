package ringbuffer

import (
	"bytes"
	"runtime"
	"sync"
	"testing"
)

func TestSetQueueBufferSize(t *testing.T) {
	tests := []struct {
		name  string
		input int
		want  int
	}{
		{
			name:  "set buffer size",
			input: 128 * 1024,
			want:  128 * 1024,
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
			got := buf.Cap()
			if tt.want != got {
				t.Errorf("Failed test: %s, want: %d, got %d", tt.name, tt.want, got)
			}
		})
	}
}
